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
     * Replay retained events after the given identifier for the requested topics.
     *
     * @param  list<string>  $topics
     * @return PromiseInterface<Replay>
     */
    public function since(EventId $after, array $topics): PromiseInterface;

    /**
     * Register a listener that receives events with their assigned identifiers.
     *
     * @param  callable(Event): void  $listener
     */
    public function subscribe(callable $listener): void;

    /**
     * Close the event log.
     */
    public function close(): void;
}
