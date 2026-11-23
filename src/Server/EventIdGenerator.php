<?php

namespace Arttecs\Murmur\Server;

use Closure;

/**
 * Generate strictly increasing event identifiers.
 */
class EventIdGenerator
{
    /**
     * The clock that returns the current time in milliseconds.
     */
    protected Closure $clock;

    /**
     * The last identifier that was generated.
     */
    protected ?EventId $lastId = null;

    /**
     * Create a new event identifier generator.
     *
     * @param  (callable(): int)|null  $clock
     */
    public function __construct(?callable $clock = null)
    {
        $this->clock = Closure::fromCallable($clock ?? fn (): int => (int) floor(microtime(true) * 1000));
    }

    /**
     * Generate the next identifier, strictly greater than the previous one.
     */
    public function next(): EventId
    {
        $ms = ($this->clock)();

        if ($this->lastId === null || $ms > $this->lastId->ms) {
            return $this->lastId = new EventId($ms, 0);
        }

        return $this->lastId = new EventId($this->lastId->ms, $this->lastId->seq + 1);
    }
}
