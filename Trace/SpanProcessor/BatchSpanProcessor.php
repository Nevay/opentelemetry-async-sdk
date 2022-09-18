<?php

declare(strict_types=1);

namespace OpenTelemetry\Async\SDK\Trace\SpanProcessor;

use function Amp\async;
use Amp\CancelledException;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use function assert;
use Closure;
use function count;
use InvalidArgumentException;
use OpenTelemetry\API\Metrics\MeterProviderInterface;
use OpenTelemetry\API\Metrics\ObservableCallbackInterface;
use OpenTelemetry\API\Metrics\ObserverInterface;
use OpenTelemetry\Async\SDK\Adapter\AmpCancellation;
use OpenTelemetry\Async\SDK\Adapter\OtelCancellation;
use OpenTelemetry\Context\Context;
use OpenTelemetry\SDK\Common\Future\CancellationInterface;
use OpenTelemetry\SDK\Trace\ReadableSpanInterface;
use OpenTelemetry\SDK\Trace\ReadWriteSpanInterface;
use OpenTelemetry\SDK\Trace\SpanDataInterface;
use OpenTelemetry\SDK\Trace\SpanExporterInterface;
use OpenTelemetry\SDK\Trace\SpanProcessorInterface;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use Revolt\EventLoop;
use Revolt\EventLoop\Suspension;
use SplQueue;
use function sprintf;
use Throwable;
use WeakReference;

/**
 * `SpanProcessor` which creates batches of spans and passes them to the
 * configured `SpanExporter` after exceeding the configured delay or batch size.
 *
 * @see https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/sdk.md#batching-processor
 */
final class BatchSpanProcessor implements SpanProcessorInterface, LoggerAwareInterface
{
    use LoggerAwareTrait;

    private const ATTRIBUTES_PROCESSOR = ['processor' => 'batching'];
    private const ATTRIBUTES_QUEUED    = self::ATTRIBUTES_PROCESSOR + ['state' => 'queued'];
    private const ATTRIBUTES_PENDING   = self::ATTRIBUTES_PROCESSOR + ['state' => 'pending'];
    private const ATTRIBUTES_PROCESSED = self::ATTRIBUTES_PROCESSOR + ['state' => 'processed'];
    private const ATTRIBUTES_DROPPED   = self::ATTRIBUTES_PROCESSOR + ['state' => 'dropped'];
    private const ATTRIBUTES_FREE      = self::ATTRIBUTES_PROCESSOR + ['state' => 'free'];

    private readonly SpanExporterInterface $spanExporter;
    private readonly int $maxQueueSize;
    private readonly float $scheduledDelay;
    private readonly float $exportTimeout;
    private readonly int $maxExportBatchSize;
    private readonly string $workerCallbackId;
    private readonly string $scheduledDelayCallbackId;

    private int $dropped = 0;
    private int $processed = 0;
    private int $queueSize = 0;
    private int $batchId = 0;
    /** @var SplQueue<list<SpanDataInterface>> */
    private SplQueue $queue;
    /** @var list<SpanDataInterface> */
    private array $batch = [];
    /** @var array<int, DeferredFuture<array<int, Future>>> */
    private array $flush = [];
    /** @var array<int, Future> */
    private array $pending = [];
    private ?Suspension $worker = null;

    private bool $closed = false;

    private ?ObservableCallbackInterface $activeExportsObserver = null;
    private ?ObservableCallbackInterface $receivedSpansObserver = null;
    private ?ObservableCallbackInterface $queueLimitObserver = null;
    private ?ObservableCallbackInterface $queueUsageObserver = null;

    /**
     * @param SpanExporterInterface $spanExporter exporter to push spans to
     * @param int $maxQueueSize maximum number of pending spans (queued and
     *        in-flight), spans exceeding this limit will be dropped
     * @param int $scheduledDelayMillis delay interval in milliseconds between
     *        two consecutive exports if `$maxExportBatchSize` is not exceeded
     * @param int $exportTimeoutMillis export timeout in milliseconds
     * @param int $maxExportBatchSize maximum batch size of every export, spans
     *        will be exported eagerly after reaching this limit
     */
    public function __construct(
        SpanExporterInterface $spanExporter,
        int $maxQueueSize = 2048,
        int $scheduledDelayMillis = 5000,
        int $exportTimeoutMillis = 30000,
        int $maxExportBatchSize = 512,
        ?MeterProviderInterface $meterProvider = null,
    ) {
        if ($maxQueueSize < 0) {
            throw new InvalidArgumentException(sprintf('Maximum queue size (%d) must be greater than or equal to zero', $maxQueueSize));
        }
        if ($scheduledDelayMillis < 0) {
            throw new InvalidArgumentException(sprintf('Scheduled delay (%d) must be greater than or equal to zero', $scheduledDelayMillis));
        }
        if ($exportTimeoutMillis < 0) {
            throw new InvalidArgumentException(sprintf('Export timeout (%d) must be greater than or equal to zero', $exportTimeoutMillis));
        }
        if ($maxExportBatchSize < 0) {
            throw new InvalidArgumentException(sprintf('Maximum export batch size (%d) must be greater than or equal to zero', $maxExportBatchSize));
        }
        if ($maxExportBatchSize > $maxQueueSize) {
            throw new InvalidArgumentException(sprintf('Maximum export batch size (%d) must be less than or equal to maximum queue size (%d)', $maxExportBatchSize, $maxQueueSize));
        }

        $this->spanExporter = $spanExporter;
        $this->maxQueueSize = $maxQueueSize;
        $this->scheduledDelay = $scheduledDelayMillis / 1000;
        $this->exportTimeout = $exportTimeoutMillis / 1000;
        $this->maxExportBatchSize = $maxExportBatchSize;

        $this->queue = new SplQueue();

        $reference = WeakReference::create($this);
        $this->workerCallbackId = EventLoop::defer(static fn () => self::worker($reference));
        $this->scheduledDelayCallbackId = EventLoop::disable(EventLoop::unreference(EventLoop::repeat(
            $this->scheduledDelay,
            static function () use ($reference): void {
                $self = $reference->get();
                assert($self instanceof self);
                $self->flush();
            },
        )));

        if (!$meterProvider) {
            return;
        }

        $meter = $meterProvider->getMeter('otel-async');
        $this->activeExportsObserver = $meter
            ->createObservableUpDownCounter(
                'otel.trace.span_processor.active_exports',
                '{exports}',
                'The number of concurrent exports that are currently in-flight',
            )
            ->observe(static function (ObserverInterface $observer) use ($reference): void {
                $self = $reference->get();
                assert($self instanceof self);
                $observer->observe(count($self->pending), self::ATTRIBUTES_PROCESSOR);
            });
        $this->receivedSpansObserver = $meter
            ->createObservableUpDownCounter(
                'otel.trace.span_processor.spans',
                '{spans}',
                'The number of sampled spans received by the span processor',
            )
            ->observe(static function (ObserverInterface $observer) use ($reference): void {
                $self = $reference->get();
                assert($self instanceof self);
                $queued = $self->queue->count() * $self->maxExportBatchSize + count($self->batch);
                $pending = $self->queueSize - $queued;
                $processed = $self->processed;
                $dropped = $self->dropped;

                $observer->observe($queued, self::ATTRIBUTES_QUEUED);
                $observer->observe($pending, self::ATTRIBUTES_PENDING);
                $observer->observe($processed, self::ATTRIBUTES_PROCESSED);
                $observer->observe($dropped, self::ATTRIBUTES_DROPPED);
            });
        $this->queueLimitObserver = $meter
            ->createObservableUpDownCounter(
                'otel.trace.span_processor.queue.limit',
                '{spans}',
                'The queue size limit',
            )
            ->observe(static function (ObserverInterface $observer) use ($reference): void {
                $self = $reference->get();
                assert($self instanceof self);
                $observer->observe($self->maxQueueSize, self::ATTRIBUTES_PROCESSOR);
            });
        $this->queueUsageObserver = $meter
            ->createObservableUpDownCounter(
                'otel.trace.span_processor.queue.usage',
                '{spans}',
                'The current queue usage',
            )
            ->observe(static function (ObserverInterface $observer) use ($reference): void {
                $self = $reference->get();
                assert($self instanceof self);
                $queued = $self->queue->count() * $self->maxExportBatchSize + count($self->batch);
                $pending = $self->queueSize - $queued;
                $free = $self->maxQueueSize - $self->queueSize;

                $observer->observe($queued, self::ATTRIBUTES_QUEUED);
                $observer->observe($pending, self::ATTRIBUTES_PENDING);
                $observer->observe($free, self::ATTRIBUTES_FREE);
            });
    }

    public function __destruct()
    {
        $this->resumeWorker();
        $this->closed = true;
        EventLoop::cancel($this->workerCallbackId);
        EventLoop::cancel($this->scheduledDelayCallbackId);

        $this->activeExportsObserver?->detach();
        $this->receivedSpansObserver?->detach();
        $this->queueLimitObserver?->detach();
        $this->queueUsageObserver?->detach();
    }

    public function onStart(ReadWriteSpanInterface $span, Context $parentContext): void
    {
        // no-op
    }

    public function onEnd(ReadableSpanInterface $span): void
    {
        if ($this->closed) {
            return;
        }
        if (!$span->getContext()->isSampled()) {
            return;
        }

        if ($this->queueSize === $this->maxQueueSize) {
            $this->dropped++;

            return;
        }

        $this->queueSize++;
        $this->batch[] = $span->toSpanData();

        if (count($this->batch) === 1) {
            EventLoop::enable($this->scheduledDelayCallbackId);
        }
        if (count($this->batch) === $this->maxExportBatchSize) {
            $this->resumeWorker();
            $this->queueBatch();
        }
    }

    public function shutdown(?CancellationInterface $cancellation = null): bool
    {
        if ($this->closed) {
            return false;
        }

        $this->closed = true;
        EventLoop::cancel($this->scheduledDelayCallbackId);

        return $this->awaitExport($this->spanExporter->shutdown(...), $cancellation);
    }

    public function forceFlush(?CancellationInterface $cancellation = null): bool
    {
        if ($this->closed) {
            return false;
        }

        return $this->awaitExport($this->spanExporter->forceFlush(...), $cancellation);
    }

    /**
     * @param WeakReference<self> $r
     */
    private static function worker(WeakReference $r): void
    {
        $p = $r->get();
        assert($p instanceof self);

        $worker = EventLoop::getSuspension();

        do {
            while (!$p->queue->isEmpty() || $p->flush) {
                if ($p->queue->isEmpty()) {
                    $p->queueBatch();
                }
                $count = count($p->queue->bottom());
                $id = ++$p->batchId;

                try {
                    $future = async($p->spanExporter
                        ->export($p->queue->dequeue(), AmpCancellation::adapt(new TimeoutCancellation($p->exportTimeout)))
                        ->await(...));
                } catch (Throwable $e) {
                    $future = Future::error($e);
                }

                $future = $future->map(static fn (bool $success) => $success or $p->logger?->warning('Export failed'));
                $future = $future->catch(static fn (Throwable $e) => $p->logger?->error('Unhandled export error', ['exception' => $e]));
                $future = $future->finally(static function () use ($count, $id, $p): void {
                    $p->processed += $count;
                    $p->queueSize -= $count;
                    unset($p->pending[$id]);
                });

                $p->pending[$id] = $future;

                /** @phan-suppress-next-line PhanNonClassMethodCall */
                ($p->flush[$id] ?? null)?->complete($p->pending);
                unset($p->flush[$id], $future, $e);
            }

            if ($p->closed) {
                return;
            }

            $p->worker = $worker;
            $p = null;
            $worker->suspend();
        } while ($p = $r->get());
    }

    private function resumeWorker(): void
    {
        if ($worker = $this->worker) {
            $this->worker = null;
            $worker->resume();
        }
    }

    private function queueBatch(): void
    {
        assert($this->batch !== []);

        $this->queue->enqueue($this->batch);
        $this->batch = [];
        EventLoop::disable($this->scheduledDelayCallbackId);
    }

    /**
     * Flushes the current batch. The returned future will be resolved with all
     * pending exports after the current batch was sent to the exporter.
     *
     * @return Future<array<int, Future>>
     */
    private function flush(): Future
    {
        if ($this->queue->isEmpty() && !$this->batch) {
            return Future::complete($this->pending);
        }

        $this->resumeWorker();
        EventLoop::disable($this->scheduledDelayCallbackId);
        $flushId = $this->batchId + $this->queue->count() + (int) (bool) $this->batch;

        return ($this->flush[$flushId] ??= new DeferredFuture())->getFuture();
    }

    /**
     * Waits until all batches are exported. The given callback is executed
     * after the current batch is {@see self::flush()}ed.
     *
     * @psalm-param Closure(?CancellationInterface=): bool $closure
     */
    private function awaitExport(Closure $closure, ?CancellationInterface $cancellation): bool
    {
        $adaptedCancellation = $cancellation
            ? OtelCancellation::adapt($cancellation)
            : null;

        try {
            $pending = $this->flush()->await($adaptedCancellation);
        } catch (CancelledException) {
            return false;
        }

        if (!$closure($cancellation)) {
            return false;
        }

        try {
            foreach (Future::iterate($pending, $adaptedCancellation) as $future) {
                $future->await();
            }
        } catch (CancelledException) {
            return false;
        }

        return true;
    }
}
