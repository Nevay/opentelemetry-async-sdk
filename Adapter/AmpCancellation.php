<?php

declare(strict_types=1);

namespace OpenTelemetry\Async\SDK\Adapter;

use Amp\Cancellation;
use Closure;
use OpenTelemetry\SDK\Common\Future\CancellationInterface;

final class AmpCancellation implements Cancellation, CancellationInterface
{
    private readonly Cancellation $cancellation;

    private function __construct(Cancellation $cancellation)
    {
        $this->cancellation = $cancellation;
    }

    public static function adapt(Cancellation $cancellation): CancellationInterface
    {
        return $cancellation instanceof CancellationInterface
            ? $cancellation
            : new self($cancellation);
    }

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
