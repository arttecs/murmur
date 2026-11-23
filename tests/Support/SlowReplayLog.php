<?php

namespace Arttecs\Murmur\Tests\Support;

use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Server\Replay;
use React\EventLoop\Loop;
use React\Promise\Promise;
use React\Promise\PromiseInterface;
use RuntimeException;

/**
 * Delay replay while delivering newly appended events immediately.
 */
class SlowReplayLog implements EventLog
{
    /**
     * The log used for storage and live delivery.
     */
    protected MemoryEventLog $log;

    /**
     * Create a log with the given delay before a replay resolves.
     */
    public function __construct(protected float $delaySeconds)
    {
        $this->log = new MemoryEventLog;
    }

    /**
     * Store and dispatch a live event.
     */
    public function append(Event $event): Event
    {
        return $this->log->append($event);
    }

    /**
     * Capture and resolve a replay after the configured delay.
     *
     * @param  list<string>  $topics
     * @return PromiseInterface<Replay>
     */
    public function since(EventId $after, array $topics, int $limit): PromiseInterface
    {
        $timer = null;

        return new Promise(function (callable $resolve) use ($after, $topics, $limit, &$timer): void {
            $timer = Loop::addTimer($this->delaySeconds, fn () => $resolve($this->log->since($after, $topics, $limit)));
        }, function () use (&$timer): void {
            Loop::cancelTimer($timer);

            throw new RuntimeException('The replay was cancelled.');
        });
    }

    /**
     * Register a listener for live events.
     *
     * @param  callable(Event): void  $listener
     */
    public function subscribe(callable $listener): void
    {
        $this->log->subscribe($listener);
    }

    /**
     * Close the underlying event log.
     */
    public function close(): void
    {
        $this->log->close();
    }
}
