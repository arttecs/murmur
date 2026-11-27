<?php

namespace Arttecs\Murmur\Tests\Support;

use React\EventLoop\LoopInterface;
use React\EventLoop\Timer\Timer;
use React\EventLoop\TimerInterface;

/**
 * Model libuv's ready timer queue when ext-uv refuses to stop inactive handles.
 */
class ReadyTimerLoop implements LoopInterface
{
    /** @var array<int, TimerInterface> */
    protected array $scheduled = [];

    /** @var list<TimerInterface> */
    protected array $ready = [];

    public function __construct(protected LoopInterface $loop) {}

    /** @return list<TimerInterface> */
    public function timers(): array
    {
        return array_values($this->scheduled);
    }

    public function queue(TimerInterface $timer): void
    {
        unset($this->scheduled[spl_object_id($timer)]);
        $this->ready[] = $timer;
    }

    public function dispatch(): void
    {
        $ready = $this->ready;
        $this->ready = [];

        foreach ($ready as $timer) {
            if ($timer->isPeriodic()) {
                $this->scheduled[spl_object_id($timer)] = $timer;
            }

            ($timer->getCallback())($timer);
        }
    }

    public function addTimer($interval, $callback): TimerInterface
    {
        $timer = new Timer($interval, $callback);

        return $this->scheduled[spl_object_id($timer)] = $timer;
    }

    public function addPeriodicTimer($interval, $callback): TimerInterface
    {
        $timer = new Timer($interval, $callback, true);

        return $this->scheduled[spl_object_id($timer)] = $timer;
    }

    public function cancelTimer(TimerInterface $timer): void
    {
        unset($this->scheduled[spl_object_id($timer)]);
    }

    public function addReadStream($stream, $listener): void
    {
        $this->loop->addReadStream($stream, $listener);
    }

    public function addWriteStream($stream, $listener): void
    {
        $this->loop->addWriteStream($stream, $listener);
    }

    public function removeReadStream($stream): void
    {
        $this->loop->removeReadStream($stream);
    }

    public function removeWriteStream($stream): void
    {
        $this->loop->removeWriteStream($stream);
    }

    public function futureTick($listener): void
    {
        $this->loop->futureTick($listener);
    }

    public function addSignal($signal, $listener): void
    {
        $this->loop->addSignal($signal, $listener);
    }

    public function removeSignal($signal, $listener): void
    {
        $this->loop->removeSignal($signal, $listener);
    }

    public function run(): void
    {
        $this->loop->run();
    }

    public function stop(): void
    {
        $this->loop->stop();
    }
}
