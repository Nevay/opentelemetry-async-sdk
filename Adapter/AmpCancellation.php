<?php

/** @noinspection PhpVarTagWithoutVariableNameInspection */

declare(strict_types=1);

namespace Nevay\Otel\Async\SDK\Adapter;

use Amp\Cancellation;
use Closure;
use OpenTelemetry\SDK\Common\Future\CancellationInterface;

/**
 * @interal
 */
final class AmpCancellation implements Cancellation, CancellationInterface
{
    private readonly Cancellation $cancellation;

    private function __construct(Cancellation $cancellation)
    {
        $this->cancellation = $cancellation;
    }

    public static function adapt(?Cancellation $cancellation): ?CancellationInterface
    {
        return !$cancellation || $cancellation instanceof CancellationInterface
            ? $cancellation
            : new self($cancellation);
    }

    /**
     * @psalm-param Closure(\Amp\CancelledException) $callback
     */
    public function subscribe(Closure $callback): string
    {
        return $this->cancellation->subscribe($callback);
    }

    public function unsubscribe(string $id): void
    {
        $this->cancellation->unsubscribe($id);
    }

    public function isRequested(): bool
    {
        return $this->cancellation->isRequested();
    }

    public function throwIfRequested(): void
    {
        $this->cancellation->throwIfRequested();
    }
}
