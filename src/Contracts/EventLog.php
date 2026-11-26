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
     * Sample the current tail before querying history and return null for an
     * empty log. The sampled tail must cover every identifier already delivered
     * to listeners when the sample is taken. With no requested topics, return
     * the tail and no events.
     * Replay events up to that tail, applying the optional filter before
     * counting them against the limit.
     *
     * @param  list<string>  $topics
     * @param  (callable(Event): bool)|null  $filter
     * @return PromiseInterface<Replay>
     */
    public function since(EventId $after, array $topics, int $limit, ?callable $filter = null): PromiseInterface;

    /**
     * Register a listener that receives events with their assigned identifiers.
     *
     * Deliver in strictly increasing identifier order across all topics. Never
     * deliver an identifier at or below one already delivered to the listener.
     * Otherwise a numbered heartbeat may cause an idle connection to skip events.
     *
     * @param  callable(Event): void  $listener
     */
    public function subscribe(callable $listener): void;

    /**
     * Register a listener for history lost during live delivery.
     *
     * @param  callable(): void  $listener
     */
    public function onGap(callable $listener): void;

    /**
     * Wait until the log can deliver live events to its subscribers. Reject if closed.
     *
     * @return PromiseInterface<null>
     */
    public function ready(): PromiseInterface;

    /**
     * Report whether the log can currently deliver live events without performing I/O.
     */
    public function healthy(): bool;

    /**
     * Close the event log.
     */
    public function close(): void;
}
