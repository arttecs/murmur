<?php

namespace Arttecs\Murmur\Tests\Support;

use React\EventLoop\LoopInterface;
use React\EventLoop\TimerInterface;

/**
 * Wraps a loop and fires every timer early, as extension loops occasionally do.
 */
class EarlyTimerLoop implements LoopInterface
{
    /**
     * The fraction of each interval the wrapped loop actually waits.
     */
    protected const FRACTION = 0.9;

    /**
     * The timers that are scheduled and have neither fired nor been cancelled.
     *
     * @var array<int, TimerInterface>
     */
    protected array $pending = [];

    /**
     * Create a loop that shortens the timers of the given loop.
     */
    public function __construct(protected LoopInterface $loop) {}

    /**
     * Count the one-shot timers that are still scheduled.
     */
    public function pendingTimers(): int
    {
        return count($this->pending);
    }

    /**
     * Watch a stream for readability on the wrapped loop.
     */
    public function addReadStream($stream, $listener): void
    {
        $this->loop->addReadStream($stream, $listener);
    }

    /**
     * Watch a stream for writability on the wrapped loop.
     */
    public function addWriteStream($stream, $listener): void
    {
        $this->loop->addWriteStream($stream, $listener);
    }

    /**
     * Stop watching a stream for readability.
     */
    public function removeReadStream($stream): void
    {
        $this->loop->removeReadStream($stream);
    }

    /**
     * Stop watching a stream for writability.
     */
    public function removeWriteStream($stream): void
    {
        $this->loop->removeWriteStream($stream);
    }

    /**
     * Schedule a one-shot timer that fires before its interval has passed.
     */
    public function addTimer($interval, $callback): TimerInterface
    {
        $timer = $this->loop->addTimer($interval * static::FRACTION, function (TimerInterface $timer) use ($callback): void {
            unset($this->pending[spl_object_id($timer)]);
            $callback($timer);
        });

        return $this->pending[spl_object_id($timer)] = $timer;
    }

    /**
     * Schedule a periodic timer that fires before each interval has passed.
     */
    public function addPeriodicTimer($interval, $callback): TimerInterface
    {
        return $this->loop->addPeriodicTimer($interval * static::FRACTION, $callback);
    }

    /**
     * Cancel a timer on the wrapped loop.
     */
    public function cancelTimer(TimerInterface $timer): void
    {
        unset($this->pending[spl_object_id($timer)]);
        $this->loop->cancelTimer($timer);
    }

    /**
     * Defer a callback to the next tick of the wrapped loop.
     */
    public function futureTick($listener): void
    {
        $this->loop->futureTick($listener);
    }

    /**
     * Register a signal listener on the wrapped loop.
     */
    public function addSignal($signal, $listener): void
    {
        $this->loop->addSignal($signal, $listener);
    }

    /**
     * Remove a signal listener from the wrapped loop.
     */
    public function removeSignal($signal, $listener): void
    {
        $this->loop->removeSignal($signal, $listener);
    }

    /**
     * Run the wrapped loop.
     */
    public function run(): void
    {
        $this->loop->run();
    }

    /**
     * Stop the wrapped loop.
     */
    public function stop(): void
    {
        $this->loop->stop();
    }
}
