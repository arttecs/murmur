<?php

namespace Arttecs\Murmur\Server;

/**
 * Hold replayed events and indicate whether the requested history has a gap.
 */
class Replay
{
    /**
     * Create a new replay result.
     *
     * @param  list<Event>  $events
     */
    public function __construct(public readonly bool $gap, public readonly array $events, public readonly ?EventId $tail) {}
}
