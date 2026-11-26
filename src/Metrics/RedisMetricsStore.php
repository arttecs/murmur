<?php

namespace Arttecs\Murmur\Metrics;

use Arttecs\Murmur\Contracts\MetricsStore;
use Illuminate\Redis\Connections\Connection;
use RuntimeException;

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
     *
     * @throws RuntimeException
     */
    public function put(string $node, int $recordedAt, array $samples): void
    {
        if ($this->connection->hset($this->key, $node, Snapshot::encode($recordedAt, $samples)) === false) {
            throw new RuntimeException('The Murmur metrics key does not hold a Redis hash.');
        }
    }

    /**
     * {@inheritdoc}
     *
     * @throws RuntimeException
     */
    public function all(): array
    {
        $snapshots = [];
        $entries = $this->connection->hgetall($this->key);

        if (! is_array($entries)) {
            throw new RuntimeException('The Murmur metrics key does not hold a Redis hash.');
        }

        foreach ($entries as $node => $json) {
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
