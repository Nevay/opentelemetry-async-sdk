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
 * @see https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/sdk.md#batching-processor
 */
final class BatchSpanProcessor implements SpanProcessorInterface, LoggerAwareInterface
{
    use LoggerAwareTrait;

    private readonly SpanExporterInterface $spanExporter;
    private readonly int $maxQueueSize;
    private readonly float $scheduledDelay;
    private readonly float $exportTimeout;
    private readonly int $maxExportBatchSize;
    private readonly string $workerCallbackId;
    private readonly string $scheduledDelayCallbackId;

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

    public function __construct(
        SpanExporterInterface $spanExporter,
        int $maxQueueSize = 2048,
        int $scheduledDelayMillis = 5000,
        int $exportTimeoutMillis = 30000,
        int $maxExportBatchSize = 512,
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
    }

    public function __destruct()
    {
        $this->resumeWorker();
        $this->closed = true;
        EventLoop::cancel($this->workerCallbackId);
        EventLoop::cancel($this->scheduledDelayCallbackId);
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
            return;
        }

        $this->queueSize++;
        $this->batch[] = $span->toSpanData();
        $this->scheduleFlush();

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
            $p->worker = null;

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

                $future = $future->catch(static fn (Throwable $e) => $p->logger?->error('Unhandled export error', ['exception' => $e]));
                $future = $future->finally(static function () use ($count, $id, $p): void {
                    $p->queueSize -= $count;
                    unset($p->pending[$id]);
                });

                $p->pending[$id] = $future;

                /** @phan-suppress-next-line PhanNonClassMethodCall */
                ($p->flush[$id] ?? null)?->complete($p->pending);
                unset($p->flush[$id], $future, $e);
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

    private function flushId(): int
    {
        return $this->batchId + $this->queue->count() + (int) (bool) $this->batch;
    }

    private function scheduleFlush(): void
    {
        assert($this->batch !== []);

        if (!isset($this->flush[$this->flushId()])) {
            EventLoop::enable($this->scheduledDelayCallbackId);
        }
    }

    /**
     * Indicates that the current batch should be flushed. Returns a future that
     * will be resolved after the current batch was sent to the exporter.
     *
     * @return Future<array<int, Future>>
     */
    private function flush(): Future
    {
        if ($this->queue->isEmpty() && !$this->batch) {
            static $empty;

            return $empty ??= Future::complete([]);
        }

        $this->resumeWorker();
        EventLoop::disable($this->scheduledDelayCallbackId);

        return ($this->flush[$this->flushId()] ??= new DeferredFuture())->getFuture();
    }

    /**
     * Waits until the current batch is exported. The given callback will be
     * executed after the current batch was {@see self::flush()}ed.
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
