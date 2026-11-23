<?php

namespace Arttecs\Murmur\Contracts;

use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Replay;
use React\Promise\PromiseInterface;

/**
 * Provide event replay and subscriptions for live events.
 */
interface EventLog
{
    /**
     * Replay up to the given number of events after the cursor, or report a gap.
     *
     * @param  list<string>  $topics
     * @return PromiseInterface<Replay>
     */
    public function since(EventId $after, array $topics, int $limit): PromiseInterface;

    /**
     * Register a listener that receives events with their assigned identifiers.
     *
     * @param  callable(Event): void  $listener
     */
    public function subscribe(callable $listener): void;

    /**
     * Wait until the log can deliver live events to its subscribers.
     *
     * @return PromiseInterface<null>
     */
    public function ready(): PromiseInterface;

    /**
     * Close the event log.
     */
    public function close(): void;
}
