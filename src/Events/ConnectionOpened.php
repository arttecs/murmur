<?php

namespace Arttecs\Murmur\Events;

/**
 * An authorized SSE connection attached to a Murmur server.
 */
readonly class ConnectionOpened
{
    /**
     * Create an event with the server's connection and topic identifiers.
     *
     * @param  list<string>  $topics
     */
    public function __construct(
        public string $connectionId,
        public string $node,
        public array $topics,
        public int $occurredAt,
    ) {}
}
