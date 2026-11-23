<?php

namespace Arttecs\Murmur\Server;

class EventId
{
    public function __construct(public readonly int $ms, public readonly int $seq) {}

    public static function tryParse(string $value): ?EventId
    {
        if (preg_match('/\A([0-9]+)-([0-9]+)\z/', $value, $matches) !== 1) {
            return null;
        }

        return new self((int) $matches[1], (int) $matches[2]);
    }

    public function compare(EventId $other): int
    {
        return ($this->ms <=> $other->ms) ?: ($this->seq <=> $other->seq);
    }

    public function __toString(): string
    {
        return "{$this->ms}-{$this->seq}";
    }
}
