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
 * `SpanProcessor` which passes finished spans to the configured `SpanExporter`
 * as soon as they are finished.
 *
 * @see https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/sdk.md#simple-processor
 */
final class SimpleSpanProcessor implements SpanProcessorInterface, LoggerAwareInterface
{
    use LoggerAwareTrait;

    private readonly SpanExporterInterface $spanExporter;
    private readonly int $maxQueueSize;
    private readonly float $exportTimeout;
    private readonly string $workerCallbackId;

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
    }

    public function __destruct()
    {
        $this->resumeWorker();
        $this->closed = true;
        EventLoop::cancel($this->workerCallbackId);
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

        do {
            while (!$p->queue->isEmpty()) {
                $id = ++$p->batchId;

                try {
                    $future = async($p->spanExporter
                        ->export([$p->queue->dequeue()], AmpCancellation::adapt(new TimeoutCancellation($p->exportTimeout)))
                        ->await(...));
                } catch (Throwable $e) {
                    $future = Future::error($e);
                }

                $future = $future->catch(static fn (Throwable $e) => $p->logger?->error('Unhandled export error', ['exception' => $e]));
                $future = $future->finally(static function () use ($id, $p): void {
                    $p->queueSize--;
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

    /**
     * Flushes the current batch. The returned future will be resolved with all
     * pending exports after the current batch was sent to the exporter.
     *
     * @return Future<array<int, Future>>
     */
    private function flush(): Future
    {
        if ($this->queue->isEmpty()) {
            return Future::complete($this->pending);
        }

        $this->resumeWorker();
        $flushId = $this->batchId + $this->queue->count();

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
