<?php

namespace Arttecs\Murmur\Metrics;

use Arttecs\Murmur\Contracts\MetricsStore;
use Closure;
use Illuminate\Contracts\Cache\LockProvider;
use Illuminate\Contracts\Cache\Repository;
use RuntimeException;

/**
 * Store expiring snapshots and a node index in a Laravel cache store.
 *
 * @internal
 */
class CacheMetricsStore implements MetricsStore
{
    /**
     * Use the given cache repository, key prefix, and expiry in seconds.
     */
    public function __construct(protected Repository $cache, protected string $prefix, protected int $ttl) {}

    /**
     * {@inheritdoc}
     */
    public function put(string $node, int $recordedAt, array $samples): void
    {
        $json = Snapshot::encode($recordedAt, $samples);

        $this->updateIndex(function (array $nodes) use ($node, $json): array {
            if (! $this->cache->put($this->prefix.'node:'.$node, $json, $this->ttl)) {
                throw new RuntimeException('Murmur could not write the metrics cache entry.');
            }

            return in_array($node, $nodes, true) ? $nodes : [...$nodes, $node];
        });
    }

    /**
     * {@inheritdoc}
     */
    public function all(): array
    {
        $snapshots = [];

        foreach ($this->nodes() as $node) {
            $snapshot = Snapshot::decode($this->cache->get($this->prefix.'node:'.$node));

            if ($snapshot !== null) {
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
        $this->updateIndex(function (array $nodes) use ($node): array {
            $this->cache->forget($this->prefix.'node:'.$node);

            return array_values(array_diff($nodes, [$node]));
        });
    }

    /**
     * Read the valid node names from the index.
     *
     * @return list<string>
     */
    protected function nodes(): array
    {
        $nodes = $this->cache->get($this->prefix.'index');

        return is_array($nodes) && array_is_list($nodes)
            ? array_values(array_filter($nodes, Snapshot::validNode(...)))
            : [];
    }

    /**
     * Serialize index updates with an atomic lock when available. Otherwise concurrent writes are last-writer-wins.
     *
     * @param  Closure(list<string>): list<string>  $update
     */
    protected function updateIndex(Closure $update): void
    {
        $write = function () use ($update): void {
            $nodes = array_values(array_filter($this->nodes(), fn (string $node): bool => $this->cache->has($this->prefix.'node:'.$node)));

            if (! $this->cache->put($this->prefix.'index', $update($nodes), $this->ttl)) {
                throw new RuntimeException('Murmur could not write the metrics cache index.');
            }
        };

        $store = $this->cache->getStore();

        if ($store instanceof LockProvider) {
            $store->lock($this->prefix.'lock', 10)->block(5, $write);
        } else {
            $write();
        }
    }
}
