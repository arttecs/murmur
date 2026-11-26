<?php

namespace Arttecs\Murmur\Contracts;

/**
 * Store the most recent metrics snapshot for each server node.
 *
 * @phpstan-type Sample array{name: string, type: 'counter'|'gauge', help: string, labels: array<string, string>, value: int|float}
 * @phpstan-type Snapshot array{recorded_at: int, samples: list<Sample>}
 */
interface MetricsStore
{
    /**
     * Replace a node's snapshot, recorded at a Unix timestamp in seconds.
     *
     * @param  list<Sample>  $samples
     */
    public function put(string $node, int $recordedAt, array $samples): void;

    /**
     * Get the snapshots keyed by node name.
     *
     * @return array<string, Snapshot>
     */
    public function all(): array;

    /**
     * Remove one node's snapshot.
     */
    public function forget(string $node): void;
}
