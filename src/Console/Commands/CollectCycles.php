<?php

namespace Arttecs\Murmur\Console\Commands;

use Arttecs\Murmur\Server\Metrics;

/**
 * Collect PHP cycles and record how many were collected.
 *
 * @internal
 */
class CollectCycles
{
    /**
     * Use the metrics of the server running the collection.
     */
    public function __construct(protected Metrics $metrics) {}

    /**
     * Run one collection and increment the cycle counter.
     *
     * @param  (callable(): int)|null  $collect
     */
    public function __invoke(?callable $collect = null): void
    {
        $this->metrics->increment('murmur_gc_collected_cycles_total', $collect === null ? gc_collect_cycles() : $collect());
    }
}
