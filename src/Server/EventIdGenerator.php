<?php

namespace Arttecs\Murmur\Server;

use Closure;

class EventIdGenerator
{
    protected Closure $clock;

    protected ?EventId $lastId = null;

    public function __construct(?callable $clock = null)
    {
        $this->clock = Closure::fromCallable($clock ?? fn (): int => (int) floor(microtime(true) * 1000));
    }

    public function next(): EventId
    {
        $ms = ($this->clock)();

        if ($this->lastId === null || $ms > $this->lastId->ms) {
            return $this->lastId = new EventId($ms, 0);
        }

        return $this->lastId = new EventId($this->lastId->ms, $this->lastId->seq + 1);
    }
}
