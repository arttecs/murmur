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
     * Schedule the first invocation.
     *
     * Use the same one-millisecond minimum repeat interval as ExtUvLoop.
     *
     * @param  Closure(): void  $callback
     */
    public function __construct(
        protected LoopInterface $loop,
        protected float $interval,
        protected Closure $callback,
    ) {
        if ($loop instanceof ExtUvLoop) {
            $this->interval = max(0.001, $interval);
        }

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
     * Schedule the next tick before calling the listener, which may cancel it.
     */
    protected function schedule(float $delay): void
    {
        $this->timer = $this->loop->addTimer($delay, function (TimerInterface $timer): void {
            if ($this->timer !== $timer) {
                return;
            }

            $this->schedule($this->interval);
            ($this->callback)();
        });
    }
}
