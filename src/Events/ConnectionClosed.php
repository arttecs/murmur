<?php

namespace Arttecs\Murmur\Events;

/**
 * An attached SSE connection closed on a Murmur server.
 */
readonly class ConnectionClosed
{
    /**
     * Create an event with the server's connection, topic, and closure details.
     *
     * @param  list<string>  $topics
     * @param  'client'|'expired'|'slow_consumer'|'shutdown'|'error'  $reason
     */
    public function __construct(
        public string $connectionId,
        public string $node,
        public array $topics,
        public int $occurredAt,
        public float $durationSeconds,
        public string $reason,
    ) {}
}
