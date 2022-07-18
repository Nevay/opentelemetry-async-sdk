<?php

declare(strict_types=1);

namespace OpenTelemetry\Async\SDK\Trace\SpanProcessor;

use function Amp\async;
use Amp\Cancellation;
use Amp\Future;
use OpenTelemetry\Async\SDK\Trace\SpanProcessorInterface;
use OpenTelemetry\Context\Context;
use OpenTelemetry\SDK\Trace\ReadableSpanInterface;
use OpenTelemetry\SDK\Trace\ReadWriteSpanInterface;

final class MultiSpanProcessor implements SpanProcessorInterface
{
    private iterable $spanProcessors;

    /**
     * @param iterable<SpanProcessorInterface> $spanProcessors
     */
    public function __construct(iterable $spanProcessors)
    {
        $this->spanProcessors = $spanProcessors;
    }

    public function onStart(ReadWriteSpanInterface $span, ?Context $parentContext = null): void
    {
        foreach ($this->spanProcessors as $spanProcessor) {
            $spanProcessor->onStart($span, $parentContext);
        }
    }

    public function onEnd(ReadableSpanInterface $span): void
    {
        foreach ($this->spanProcessors as $spanProcessor) {
            $spanProcessor->onEnd($span);
        }
    }

    public function shutdown(?Cancellation $cancellation = null): bool
    {
        $futures = [];
        foreach ($this->spanProcessors as $spanProcessor) {
            $futures[] = async($spanProcessor->shutdown(...), $cancellation);
        }

        $success = true;
        foreach (Future::iterate($futures) as $future) {
            if (!$future->await()) {
                $success = false;
            }
        }

        return $success;
    }

    public function forceFlush(?Cancellation $cancellation = null): bool
    {
        $futures = [];
        foreach ($this->spanProcessors as $spanProcessor) {
            $futures[] = async($spanProcessor->forceFlush(...), $cancellation);
        }

        $success = true;
        foreach (Future::iterate($futures) as $future) {
            if (!$future->await()) {
                $success = false;
            }
        }

        return $success;
    }
}
