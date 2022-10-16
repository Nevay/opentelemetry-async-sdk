<?php

declare(strict_types=1);

namespace OpenTelemetry\Async\SDK\Trace\SpanProcessor;

use Amp\CancelledException;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use function array_intersect_key;
use function array_keys;
use function assert;
use Closure;
use function count;
use InvalidArgumentException;
use OpenTelemetry\API\Metrics\MeterProviderInterface;
use OpenTelemetry\API\Metrics\ObservableCallbackInterface;
use OpenTelemetry\API\Metrics\ObserverInterface;
use OpenTelemetry\Async\SDK\Adapter\AmpCancellation;
use OpenTelemetry\Async\SDK\Adapter\OtelCancellation;
use OpenTelemetry\Context\ContextInterface;
use OpenTelemetry\SDK\Common\Future\CancellationInterface;
use OpenTelemetry\SDK\Common\Future\ErrorFuture;
use OpenTelemetry\SDK\Common\Future\FutureInterface;
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
 * `SpanProcessor` which passes finished spans to the configured `SpanExporter`
 * as soon as they are finished.
 *
 * @see https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/sdk.md#simple-processor
 */
final class SimpleSpanProcessor implements SpanProcessorInterface, LoggerAwareInterface
{
    use LoggerAwareTrait;

    private const ATTRIBUTES_PROCESSOR = ['processor' => 'simple'];
    private const ATTRIBUTES_QUEUED    = self::ATTRIBUTES_PROCESSOR + ['state' => 'queued'];
    private const ATTRIBUTES_PENDING   = self::ATTRIBUTES_PROCESSOR + ['state' => 'pending'];
    private const ATTRIBUTES_PROCESSED = self::ATTRIBUTES_PROCESSOR + ['state' => 'processed'];
    private const ATTRIBUTES_DROPPED   = self::ATTRIBUTES_PROCESSOR + ['state' => 'dropped'];
    private const ATTRIBUTES_FREE      = self::ATTRIBUTES_PROCESSOR + ['state' => 'free'];

    private readonly SpanExporterInterface $spanExporter;
    private readonly int $maxQueueSize;
    private readonly float $exportTimeout;
    private readonly string $workerCallbackId;

    private int $dropped = 0;
    private int $processed = 0;
    private int $queueSize = 0;
    private int $batchId = 0;
    /** @var SplQueue<SpanDataInterface> */
    private SplQueue $queue;
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
     * @param int $exportTimeoutMillis export timeout in milliseconds
     */
    public function __construct(
        SpanExporterInterface $spanExporter,
        int $maxQueueSize = 2048,
        int $exportTimeoutMillis = 30000,
        ?MeterProviderInterface $meterProvider = null,
    ) {
        if ($maxQueueSize < 0) {
            throw new InvalidArgumentException(sprintf('Maximum queue size (%d) must be greater than or equal to zero', $maxQueueSize));
        }
        if ($exportTimeoutMillis < 0) {
            throw new InvalidArgumentException(sprintf('Export timeout (%d) must be greater than or equal to zero', $exportTimeoutMillis));
        }

        $this->spanExporter = $spanExporter;
        $this->maxQueueSize = $maxQueueSize;
        $this->exportTimeout = $exportTimeoutMillis / 1000;

        $this->queue = new SplQueue();

        $reference = WeakReference::create($this);
        $this->workerCallbackId = EventLoop::defer(static fn () => self::worker($reference));

        if (!$meterProvider) {
            return;
        }

        $meter = $meterProvider->getMeter('io.opentelemetry.sdk.async');
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
                'The number of received spans',
            )
            ->observe(static function (ObserverInterface $observer) use ($reference): void {
                $self = $reference->get();
                assert($self instanceof self);
                $queued = $self->queue->count();
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
                $queued = $self->queue->count();
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

        $this->activeExportsObserver?->detach();
        $this->receivedSpansObserver?->detach();
        $this->queueLimitObserver?->detach();
        $this->queueUsageObserver?->detach();
    }

    public function onStart(ReadWriteSpanInterface $span, ContextInterface $parentContext): void
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
        $this->queue->enqueue($span->toSpanData());
        $this->resumeWorker();
    }

    public function shutdown(?CancellationInterface $cancellation = null): bool
    {
        if ($this->closed) {
            return false;
        }

        $this->closed = true;

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
        $handler = static function (DeferredFuture $deferred, FutureInterface $future, self $processor, int $id): void {
            $d = $deferred;
            $f = $future;
            $p = $processor;
            unset($deferred, $future, $processor);

            $success = $e = null;

            try {
                $success = $f->await();
            } catch (Throwable $e) {
            } finally {
                $d->complete();
                $p->processed++;
                $p->queueSize--;
                unset($p->pending[$id]);
            }

            if (!$logger = $p->logger) {
                return;
            }

            unset($d, $f, $p);
            match ($success) {
                true => $logger->info('Export successful', ['id' => $id]),
                false => $logger->warning('Export failed', ['id' => $id]),
                default => $logger->error('Unhandled export error', ['id' => $id, 'exception' => $e]),
            };
        };

        do {
            while (!$p->queue->isEmpty()) {
                $id = ++$p->batchId;

                $deferred = new DeferredFuture();
                $p->pending[$id] = $deferred->getFuture();
                ($p->flush[$id] ?? null)?->complete($p->pending);

                try {
                    $future = $p->spanExporter->export(
                        [$p->queue->dequeue()],
                        AmpCancellation::adapt(new TimeoutCancellation($p->exportTimeout)),
                    );
                } catch (Throwable $e) {
                    $future = new ErrorFuture($e);
                }

                EventLoop::queue($handler, $deferred, $future, $p, $id);
                unset($p->flush[$id], $deferred, $future, $e);
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
        $this->worker?->resume();
        $this->worker = null;
    }

    private function flushId(): int
    {
        return $this->batchId + $this->queue->count();
    }

    /**
     * Flushes the batch. The returned future will be resolved with all
     * pending exports after the batch was sent to the exporter.
     *
     * @return Future<array<int, Future>>
     * @see self::flushId()
     */
    private function flush(int $flushId): Future
    {
        $this->resumeWorker();

        return $flushId === $this->batchId
            ? Future::complete($this->pending)
            : ($this->flush[$flushId] ??= new DeferredFuture())->getFuture();
    }

    /**
     * Waits until all batches are exported. The given callback is executed
     * after the current batch is {@see self::flush()}ed.
     *
     * @psalm-param Closure(?CancellationInterface=): bool $closure
     */
    private function awaitExport(Closure $closure, ?CancellationInterface $cancellation): bool
    {
        $c = $closure;
        $o = $cancellation;
        $a = OtelCancellation::adapt($cancellation);
        unset($closure, $cancellation);

        $flushId = $this->flushId();

        try {
            $pending = $this->flush($flushId)->await($a);
        } catch (CancelledException $e) {
            $this->logger?->warning('Flush cancelled, not all queued spans were processed', [
                'exception' => $e,
                'skipped_batches' => $flushId - $this->batchId,
                'requested_id' => $flushId,
                'processed_id' => $this->batchId,
            ]);
        }

        try {
            $success = $c($o);
        } catch (Throwable $e) {
            $this->logger?->error('Flush cancelled, unhandled exporter error', ['exception' => $e]);
        }
        if (!isset($pending, $success) || !$success) {
            return false;
        }

        try {
            foreach (Future::iterate($pending, $a) as $future) {
                $future->await();
            }
        } catch (CancelledException $e) {
            $this->logger?->info('Flush cancelled, not all pending exports were awaited', [
                'exception' => $e,
                /** @phan-suppress-next-line PhanTypeMismatchArgumentInternal */
                'pending_exports' => array_keys(array_intersect_key($pending, $this->pending)),
            ]);

            return false;
        }

        return true;
    }
}
