<?php

declare(strict_types=1);

namespace OpenTelemetry\Async\SDK\Trace\SpanProcessor;

use function Amp\async;
use Amp\CancelledException;
use Amp\Future;
use Amp\TimeoutCancellation;
use function Amp\weakClosure;
use function array_key_last;
use function assert;
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
use Revolt\EventLoop;
use function sprintf;

/**
 * @see https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/sdk.md#batching-processor
 */
final class BatchSpanProcessor implements SpanProcessorInterface
{
    private readonly SpanExporterInterface $spanExporter;
    private readonly int $maxQueueSize;
    private readonly float $scheduledDelay;
    private readonly float $exportTimeout;
    private readonly int $maxExportBatchSize;
    private readonly string $scheduledDelayCallbackId;

    private int $queueSize = 0;
    /** @var list<SpanDataInterface> */
    private array $batch = [];
    /** @var array<int, Future> */
    private array $pending = [];

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

        $this->scheduledDelayCallbackId = EventLoop::unreference(EventLoop::repeat(
            $this->scheduledDelay,
            weakClosure($this->queueBatch(...)),
        ));
    }

    public function __destruct()
    {
        $this->closed = true;
        EventLoop::cancel($this->scheduledDelayCallbackId);
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

        if ($this->queueSize === $this->maxQueueSize) {
            return;
        }

        $this->queueSize++;
        $this->batch[] = $span->toSpanData();
        if (count($this->batch) === $this->maxExportBatchSize) {
            $this->queueBatch();
        }
    }

    public function shutdown(?CancellationInterface $cancellation = null): bool
    {
        if ($this->closed) {
            return false;
        }

        $this->queueBatch();
        $this->__destruct();

        $shutdown = async($this->spanExporter->shutdown(...), $cancellation);

        return $this->awaitPending($cancellation) && $shutdown->await();
    }

    public function forceFlush(?CancellationInterface $cancellation = null): bool
    {
        if ($this->closed) {
            return false;
        }

        $this->queueBatch();

        $forceFlush = async($this->spanExporter->forceFlush(...), $cancellation);

        return $this->awaitPending($cancellation) && $forceFlush->await();
    }

    private function queueBatch(): void
    {
        assert(!$this->closed);
        if (!$batch = $this->batch) {
            return;
        }

        $this->batch = [];
        EventLoop::disable($this->scheduledDelayCallbackId);
        EventLoop::enable($this->scheduledDelayCallbackId);

        $id = array_key_last($this->pending) + 1;
        $this->pending[$id] = async(function (array $batch, int $id): int {
            try {
                /** @var list<SpanDataInterface> $batch */
                return $this->spanExporter->export($batch, AmpCancellation::adapt(new TimeoutCancellation($this->exportTimeout)));
            } finally {
                $this->queueSize -= count($batch);
                unset($this->pending[$id]);
            }
        }, $batch, $id);
    }

    private function awaitPending(?CancellationInterface $cancellation): bool
    {
        if ($cancellation) {
            $cancellation = OtelCancellation::adapt($cancellation);
        }

        try {
            foreach (Future::iterate($this->pending, $cancellation) as $future) {
                $future->await();
            }
        } catch (CancelledException) {
            return false;
        }

        return true;
    }
}
