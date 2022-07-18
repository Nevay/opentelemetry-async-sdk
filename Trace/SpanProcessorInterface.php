<?php

declare(strict_types=1);

namespace OpenTelemetry\Async\SDK\Trace;

use Amp\Cancellation;

interface SpanProcessorInterface extends \OpenTelemetry\SDK\Trace\SpanProcessorInterface
{
    public function shutdown(?Cancellation $cancellation = null): bool;

    public function forceFlush(?Cancellation $cancellation = null): bool;
}
