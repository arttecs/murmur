<?php

namespace Arttecs\Murmur\Metrics;

use Arttecs\Murmur\Contracts\MetricsStore;
use Illuminate\Redis\Connections\Connection;

/**
 * Store node snapshots as fields in a Redis hash.
 */
class RedisMetricsStore implements MetricsStore
{
    /**
     * Use the given Redis connection and unprefixed hash key.
     */
    public function __construct(protected Connection $connection, protected string $key) {}

    /**
     * {@inheritdoc}
     */
    public function put(string $node, int $recordedAt, array $samples): void
    {
        $this->connection->hset($this->key, $node, Snapshot::encode($recordedAt, $samples));
    }

    /**
     * {@inheritdoc}
     */
    public function all(): array
    {
        $snapshots = [];

        foreach ($this->connection->hgetall($this->key) as $node => $json) {
            if (Snapshot::validNode((string) $node) && ($snapshot = Snapshot::decode($json)) !== null) {
                $snapshots[$node] = $snapshot;
            }
        }

        return $snapshots;
    }

    /**
     * {@inheritdoc}
     */
    public function forget(string $node): void
    {
        $this->connection->hdel($this->key, $node);
    }
}
