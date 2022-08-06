<?php

/** @noinspection PhpVarTagWithoutVariableNameInspection */

declare(strict_types=1);

namespace OpenTelemetry\Async\SDK\Adapter;

use Amp\Cancellation;
use Amp\DeferredCancellation;
use Closure;
use OpenTelemetry\SDK\Common\Future\CancellationInterface;

final class OtelCancellation implements Cancellation, CancellationInterface
{
    private readonly CancellationInterface $cancellation;
    private readonly string $id;

    private readonly Cancellation $deferred;

    private function __construct(CancellationInterface $cancellation)
    {
        $this->cancellation = $cancellation;

        $deferred = new DeferredCancellation();
        $this->id = $this->cancellation->subscribe($deferred->cancel(...));
        $this->deferred = $deferred->getCancellation();
    }

    public static function adapt(CancellationInterface $cancellation): Cancellation
    {
        return $cancellation instanceof Cancellation
            ? $cancellation
            : new self($cancellation);
    }

    /**
     * @psalm-param Closure(\Amp\CancelledException) $callback
     */
    public function subscribe(Closure $callback): string
    {
        return $this->deferred->subscribe($callback);
    }

    public function unsubscribe(string $id): void
    {
        $this->deferred->unsubscribe($id);
    }

    public function isRequested(): bool
    {
        return $this->deferred->isRequested();
    }

    public function throwIfRequested(): void
    {
        $this->deferred->throwIfRequested();
    }

    public function __destruct()
    {
        $this->cancellation->unsubscribe($this->id);
    }
}
