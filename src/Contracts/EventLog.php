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
     * Sample the tail before querying history and return it even if no topics were
     * requested. Replay events up to that tail, applying the optional filter
     * before counting them against the limit.
     *
     * @param  list<string>  $topics
     * @param  (callable(Event): bool)|null  $filter
     * @return PromiseInterface<Replay>
     */
    public function since(EventId $after, array $topics, int $limit, ?callable $filter = null): PromiseInterface;

    /**
     * Register a listener that receives events with their assigned identifiers.
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
