<?php

declare(strict_types=1);

namespace Nevay\Otel\Async\SDK\Trace\SpanProcessor;

use function Amp\async;
use Amp\Future;
use OpenTelemetry\Context\ContextInterface;
use OpenTelemetry\SDK\Common\Future\CancellationInterface;
use OpenTelemetry\SDK\Trace\ReadableSpanInterface;
use OpenTelemetry\SDK\Trace\ReadWriteSpanInterface;
use OpenTelemetry\SDK\Trace\SpanProcessorInterface;

final class MultiSpanProcessor implements SpanProcessorInterface
{
    private readonly iterable $spanProcessors;

    /**
     * @param iterable<SpanProcessorInterface> $spanProcessors
     */
    public function __construct(iterable $spanProcessors)
    {
        $this->spanProcessors = $spanProcessors;
    }

    public function onStart(ReadWriteSpanInterface $span, ContextInterface $parentContext): void
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

    public function shutdown(?CancellationInterface $cancellation = null): bool
    {
        $futures = [];
        $shutdown = static function (SpanProcessorInterface $p, ?CancellationInterface $cancellation): bool {
            return $p->shutdown($cancellation);
        };
        foreach ($this->spanProcessors as $spanProcessor) {
            $futures[] = async($shutdown, $spanProcessor, $cancellation);
        }

        $success = true;
        foreach (Future::iterate($futures) as $future) {
            if (!$future->await()) {
                $success = false;
            }
        }

        return $success;
    }

    public function forceFlush(?CancellationInterface $cancellation = null): bool
    {
        $futures = [];
        $forceFlush = static function (SpanProcessorInterface $p, ?CancellationInterface $cancellation): bool {
            return $p->forceFlush($cancellation);
        };
        foreach ($this->spanProcessors as $spanProcessor) {
            $futures[] = async($forceFlush, $spanProcessor, $cancellation);
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
