<?php

namespace Arttecs\Murmur\Metrics;

use Arttecs\Murmur\Contracts\MetricsStore;
use Illuminate\Database\ConnectionInterface;

/**
 * Store each node's latest snapshot in one database row.
 *
 * @internal
 */
class DatabaseMetricsStore implements MetricsStore
{
    /**
     * Use the given database connection and table.
     */
    public function __construct(protected ConnectionInterface $connection, protected string $table) {}

    /**
     * {@inheritdoc}
     */
    public function put(string $node, int $recordedAt, array $samples): void
    {
        $this->connection->table($this->table)->upsert([
            'node' => $node,
            'recorded_at' => $recordedAt,
            'samples' => json_encode(Snapshot::encodeSamples($samples), JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION),
        ], ['node'], ['recorded_at', 'samples']);
    }

    /**
     * {@inheritdoc}
     */
    public function all(): array
    {
        $snapshots = [];

        foreach ($this->connection->table($this->table)->get() as $row) {
            $recordedAt = filter_var($row->recorded_at, FILTER_VALIDATE_INT);
            $samples = is_string($row->samples) ? json_decode($row->samples) : null;
            $snapshot = Snapshot::fromDecoded($recordedAt, $samples);

            if (Snapshot::validNode($row->node) && $snapshot !== null) {
                $snapshots[$row->node] = $snapshot;
            }
        }

        return $snapshots;
    }

    /**
     * {@inheritdoc}
     */
    public function forget(string $node): void
    {
        $this->connection->table($this->table)->where('node', $node)->delete();
    }
}
