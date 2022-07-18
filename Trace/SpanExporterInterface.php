<?php

declare(strict_types=1);

namespace OpenTelemetry\Async\SDK\Trace;

use Amp\Cancellation;

interface SpanExporterInterface extends \OpenTelemetry\SDK\Trace\SpanExporterInterface
{
    public function export(iterable $spans, ?Cancellation $cancellation = null): int;

    public function shutdown(?Cancellation $cancellation = null): bool;

    public function forceFlush(?Cancellation $cancellation = null): bool;
}
