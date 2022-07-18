<?php

declare(strict_types=1);

namespace OpenTelemetry\Async\SDK\Trace\SpanExporter;

use Amp\Cancellation;
use LogicException;
use OpenTelemetry\Async\SDK\Trace\SpanExporterInterface;
use OpenTelemetry\SDK\Trace\SpanExporterInterface as SyncSpanExporterInterface;
use function sprintf;

final class SpanExporterWrapper implements SpanExporterInterface
{
    private SyncSpanExporterInterface $spanExporter;

    private function __construct(SyncSpanExporterInterface $spanExporter)
    {
        $this->spanExporter = $spanExporter;
    }

    public static function wrap(SyncSpanExporterInterface $spanExporter): SpanExporterInterface
    {
        if ($spanExporter instanceof SpanExporterInterface) {
            return $spanExporter;
        }

        return new self($spanExporter);
    }

    public static function fromConnectionString(string $endpointUrl, string $name, string $args): never
    {
        throw new LogicException(sprintf('%s not supported', __FUNCTION__));
    }

    public function export(iterable $spans, ?Cancellation $cancellation = null): int
    {
        return $this->spanExporter->export($spans);
    }

    public function shutdown(?Cancellation $cancellation = null): bool
    {
        return $this->spanExporter->shutdown();
    }

    public function forceFlush(?Cancellation $cancellation = null): bool
    {
        return $this->spanExporter->forceFlush();
    }
}
