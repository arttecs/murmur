<?php

namespace Arttecs\Murmur\Tests\Feature\Conformance;

use Arttecs\Murmur\Contracts\MetricsStore;
use Arttecs\Murmur\Metrics\RedisMetricsStore;
use Arttecs\Murmur\Testing\MetricsStoreConformanceTestCase;
use Illuminate\Redis\Connections\Connection;
use Illuminate\Redis\RedisManager;
use Throwable;

class RedisMetricsStoreTest extends MetricsStoreConformanceTestCase
{
    protected ?Connection $redis = null;

    protected ?string $key = null;

    protected function createMetricsStore(): MetricsStore
    {
        $manager = new RedisManager(null, getenv('REDIS_CLIENT') ?: 'phpredis', [
            'default' => ['url' => getenv('MURMUR_TEST_REDIS_URL') ?: 'redis://127.0.0.1:6379', 'timeout' => 1.0],
        ]);

        try {
            $this->redis = $manager->connection();
            $this->redis->ping();
        } catch (Throwable $error) {
            if (getenv('CI') !== false) {
                throw $error;
            }

            $this->markTestSkipped('Redis is unreachable.');
        }

        $this->key = 'murmur:test:'.bin2hex(random_bytes(16));

        return new RedisMetricsStore($this->redis, $this->key);
    }

    protected function tearDown(): void
    {
        try {
            if ($this->key !== null) {
                $this->redis->del($this->key);
            }
        } finally {
            $this->redis?->disconnect();
            parent::tearDown();
        }
    }
}
