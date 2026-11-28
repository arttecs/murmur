<?php

namespace Arttecs\Murmur\Server;

use Closure;
use React\EventLoop\ExtUvLoop;
use React\EventLoop\LoopInterface;
use React\EventLoop\TimerInterface;

/**
 * Repeat a callback using one-shot timers that cannot repeat after cancellation.
 *
 * ext-uv may dispatch a cancelled timer that libuv already queued. A one-shot
 * timer keeps that late dispatch from rearming a native periodic handle.
 *
 * @internal
 */
class PeriodicTimer
{
    /**
     * The timer for the next invocation.
     */
    protected ?TimerInterface $timer = null;

    /**
     * The monotonic time in seconds at which the next invocation is due.
     */
    protected float $dueAt;

    /**
     * Schedule the first invocation.
     *
     * Use the same one-millisecond minimum repeat interval as ExtUvLoop. A fixed-rate
     * timer keeps its deadlines one interval apart, so a late invocation shortens the
     * wait for the next one instead of delaying every later invocation.
     *
     * @param  Closure(): void  $callback
     */
    public function __construct(
        protected LoopInterface $loop,
        protected float $interval,
        protected Closure $callback,
        protected bool $fixedRate = false,
    ) {
        if ($loop instanceof ExtUvLoop) {
            $this->interval = max(0.001, $interval);
        }

        $this->dueAt = hrtime(true) / 1_000_000_000 + $interval;
        $this->schedule($interval);
    }

    /**
     * Ignore any queued dispatch and cancel the underlying timer.
     */
    public function cancel(): void
    {
        if ($this->timer !== null) {
            $timer = $this->timer;
            $this->timer = null;
            $this->loop->cancelTimer($timer);
        }
    }

    /**
     * Get the delay before the next invocation.
     */
    protected function nextDelay(): float
    {
        if (! $this->fixedRate) {
            return $this->interval;
        }

        $this->dueAt += $this->interval;
        $minimum = $this->loop instanceof ExtUvLoop ? 0.001 : 0.0;

        return max($minimum, $this->dueAt - hrtime(true) / 1_000_000_000);
    }

    /**
     * Schedule the next tick before calling the listener, which may cancel it.
     */
    protected function schedule(float $delay): void
    {
        $this->timer = $this->loop->addTimer($delay, function (TimerInterface $timer): void {
            if ($this->timer !== $timer) {
                return;
            }

            $this->schedule($this->nextDelay());
            ($this->callback)();
        });
    }
}
