<?php

declare(strict_types=1);

namespace OpenTelemetry\Async\SDK\Trace\SpanProcessor;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredFuture;
use Amp\TimeoutCancellation;
use function count;
use function intdiv;
use OpenTelemetry\Async\SDK\Trace\SpanExporterInterface;
use OpenTelemetry\Async\SDK\Trace\SpanProcessorInterface;
use OpenTelemetry\Context\Context;
use OpenTelemetry\SDK\Trace\ReadableSpanInterface;
use OpenTelemetry\SDK\Trace\ReadWriteSpanInterface;
use OpenTelemetry\SDK\Trace\SpanDataInterface;
use Revolt\EventLoop;
use Revolt\EventLoop\Suspension;
use SplQueue;
use WeakReference;

final class BatchSpanProcessor implements SpanProcessorInterface
{
    private SpanExporterInterface $spanExporter;
    private int $maxQueueSize;
    private int $maxBatchSize;
    private float $scheduledDelay;
    private float $exportTimeout;
    private int $maxExportBatchSize;

    /** @var SplQueue<list<SpanDataInterface>> */
    private SplQueue $queue;
    /** @var list<SpanDataInterface> */
    private array $batch = [];
    private ?Suspension $worker = null;
    private ?DeferredFuture $flush = null;

    private bool $closed = false;

    public function __construct(
        SpanExporterInterface $spanExporter,
        int $maxQueueSize = 2048,
        int $scheduledDelayMillis = 5000,
        int $exportTimeoutMillis = 30000,
        int $maxExportBatchSize = 512,
    ) {
        $this->spanExporter = $spanExporter;
        $this->maxQueueSize = intdiv($maxQueueSize, $maxExportBatchSize);
        $this->maxBatchSize = $maxQueueSize % $maxExportBatchSize;
        $this->scheduledDelay = $scheduledDelayMillis / 1000;
        $this->exportTimeout = $exportTimeoutMillis / 1000;
        $this->maxExportBatchSize = $maxExportBatchSize;

        $this->queue = new SplQueue();

        EventLoop::queue(self::worker(...), WeakReference::create($this));
    }

    public function __destruct()
    {
        $this->resumeWorker();
    }

    public function onStart(ReadWriteSpanInterface $span, ?Context $parentContext = null): void
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

        if (count($this->queue) === $this->maxQueueSize && count($this->batch) === $this->maxBatchSize) {
            return;
        }

        $this->batch[] = $span->toSpanData();
        if (count($this->batch) === $this->maxExportBatchSize) {
            $this->resumeWorker();
            $this->queue->enqueue($this->batch);
            $this->batch = [];
        }
    }

    public function shutdown(?Cancellation $cancellation = null): bool
    {
        if ($this->closed) {
            return false;
        }

        $this->closed = true;

        $flush = $this->flush($cancellation);
        $shutdown = $this->spanExporter->shutdown($cancellation);

        return $flush && $shutdown;
    }

    public function forceFlush(?Cancellation $cancellation = null): bool
    {
        if ($this->closed) {
            return false;
        }

        $flush = $this->flush($cancellation);
        $forceFlush = $this->spanExporter->forceFlush($cancellation);

        return $flush && $forceFlush;
    }

    private static function worker(WeakReference $r): void
    {
        if (!$p = $r->get()) {
            return;
        }

        $worker = EventLoop::getSuspension();

        /** @var self $p */
        /** @psalm-suppress InvalidArgument @phan-suppress-next-line PhanTypeMismatchArgumentSuperType */
        $delay = EventLoop::repeat($p->scheduledDelay, $worker->resume(...));
        EventLoop::unreference($delay);
        EventLoop::disable($delay);

        do {
            $p->worker = null;
            for ($i = 0; !$p->queue->isEmpty(); $i++) {
                $p->spanExporter->export($p->queue->dequeue(), new TimeoutCancellation($p->exportTimeout));
            }
            if (($p->flush || !$i) && $batch = $p->batch) {
                $p->batch = [];
                $p->spanExporter->export($batch, new TimeoutCancellation($p->exportTimeout));
                unset($batch);
            }

            $p->flush?->complete();
            $p->flush = null;

            if ($p->closed) {
                break;
            }

            $p->worker = $worker;
            $p = null;

            EventLoop::enable($delay);
            $worker->suspend();
            EventLoop::disable($delay);
        } while ($p = $r->get());

        EventLoop::cancel($delay);
    }

    private function flush(?Cancellation $cancellation = null): bool
    {
        $this->resumeWorker();
        $this->flush ??= new DeferredFuture();

        try {
            $this->flush->getFuture()->await($cancellation);
        } catch (CancelledException) {
            return false;
        }

        return true;
    }

    private function resumeWorker(): void
    {
        if ($worker = $this->worker) {
            $this->worker = null;
            $worker->resume(true);
        }
    }
}
