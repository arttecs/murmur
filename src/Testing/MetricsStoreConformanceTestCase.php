<?php

namespace Arttecs\Murmur\Testing;

use Arttecs\Murmur\Contracts\MetricsStore;
use PHPUnit\Framework\TestCase;

/**
 * Verify a custom store against Murmur's metrics snapshot contract.
 */
abstract class MetricsStoreConformanceTestCase extends TestCase
{
    /**
     * Create an empty, isolated store for this test.
     */
    abstract protected function createMetricsStore(): MetricsStore;

    /**
     * Store snapshots for several nodes and update or remove them independently.
     */
    public function test_stores_replaces_and_forgets_snapshots_per_node(): void
    {
        $store = $this->createMetricsStore();
        $now = time();
        self::assertSame([], $store->all());

        $store->put('alpha', $now, []);
        $store->put('node:2', $now, []);
        $nodes = $store->all();
        ksort($nodes);
        self::assertSame([
            'alpha' => ['recorded_at' => $now, 'samples' => []],
            'node:2' => ['recorded_at' => $now, 'samples' => []],
        ], $nodes);

        $sample = ['name' => 'murmur_connections', 'type' => 'gauge', 'help' => 'Connections.', 'labels' => [], 'value' => 7];
        $store->put('alpha', $now + 1, [$sample]);
        self::assertEquals(['recorded_at' => $now + 1, 'samples' => [$sample]], $store->all()['alpha']);

        $store->forget('alpha');
        $store->forget('absent');
        self::assertSame(['node:2' => ['recorded_at' => $now, 'samples' => []]], $store->all());
    }

    /**
     * Preserve sample order, empty and nonempty labels, and exact integer and float values.
     */
    public function test_round_trips_samples_without_losing_labels_or_numeric_types(): void
    {
        $store = $this->createMetricsStore();
        $now = time();
        $samples = [
            ['name' => 'murmur_publish_requests_total', 'type' => 'counter', 'help' => 'Publish responses.', 'labels' => ['status' => '202'], 'value' => 7],
            ['name' => 'murmur_event_loop_lag_seconds', 'type' => 'gauge', 'help' => 'Loop lag.', 'labels' => [], 'value' => 0.125],
            ['name' => 'murmur_uptime_seconds', 'type' => 'gauge', 'help' => 'Uptime.', 'labels' => [], 'value' => 2.0],
        ];

        $store->put('alpha', $now, $samples);

        $stored = $store->all();
        self::assertSame(['alpha'], array_keys($stored));
        self::assertSame($now, $stored['alpha']['recorded_at']);
        self::assertCount(count($samples), $stored['alpha']['samples']);

        foreach ($samples as $index => $sample) {
            foreach ($sample as $key => $value) {
                self::assertSame($value, $stored['alpha']['samples'][$index][$key] ?? null, "Sample {$index} lost or changed [{$key}].");
            }
        }

        $store->put('alpha', $now + 1, []);
        self::assertSame(['alpha' => ['recorded_at' => $now + 1, 'samples' => []]], $store->all());
    }
}
