<?php

namespace Arttecs\Murmur\Server;

/**
 * A position in the event log, ordered by milliseconds and then by sequence.
 */
class EventId
{
    /**
     * Create a new event identifier.
     */
    public function __construct(public readonly int $ms, public readonly int $seq) {}

    /**
     * Parse an identifier in the "<ms>-<seq>" format, or return null when it is malformed.
     */
    public static function tryParse(string $value): ?EventId
    {
        if (preg_match('/\A([0-9]{1,18})-([0-9]{1,18})\z/', $value, $matches) !== 1) {
            return null;
        }

        return new self((int) $matches[1], (int) $matches[2]);
    }

    /**
     * Compare this identifier with another one, by milliseconds and then by sequence.
     */
    public function compare(EventId $other): int
    {
        return ($this->ms <=> $other->ms) ?: ($this->seq <=> $other->seq);
    }

    /**
     * Get the identifier in the "<ms>-<seq>" format.
     */
    public function __toString(): string
    {
        return "{$this->ms}-{$this->seq}";
    }
}
