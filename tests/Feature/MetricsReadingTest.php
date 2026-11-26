<?php

use Arttecs\Murmur\Exceptions\MetricsStoreNotConfigured;
use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\Metrics\Snapshot;
use Arttecs\Murmur\MurmurServiceProvider;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SseReader;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\Process\Process;

function storedMetric(string $name, int|float $value, array $labels = [], string $type = 'counter'): array
{
    return ['name' => $name, 'type' => $type, 'help' => 'Sample for testing.', 'labels' => $labels, 'value' => $value];
}

it('returns sorted per-node snapshots newer than the TTL', function () {
    config(['murmur.metrics.store' => 'cache', 'murmur.metrics.cache.store' => 'array']);
    $this->freezeTime();
    $now = now()->timestamp;
    $store = Murmur::metricsStore();
    $store->put('z', $now - 59, []);
    $store->put('stale', $now - 60, []);
    $store->put('a', $now, [storedMetric('murmur_errors_total', 1)]);

    expect(Murmur::metrics())->toBe([
        'a' => ['recorded_at' => $now, 'samples' => [storedMetric('murmur_errors_total', 1)]],
        'z' => ['recorded_at' => $now - 59, 'samples' => []],
    ]);
});

it('adds fresh counters and gauges by canonical series and omits uptime', function () {
    config(['murmur.metrics.store' => 'cache', 'murmur.metrics.cache.store' => 'array']);
    $store = Murmur::metricsStore();
    $now = now()->timestamp;
    $labels = ['status' => '202', 'worker' => 'a"b'];
    $store->put('a', $now, [
        storedMetric('murmur_publish_requests_total', 2, $labels),
        storedMetric('murmur_publish_requests_total', 9, ['status' => '404']),
        storedMetric('murmur_connections', 3, type: 'gauge'),
        storedMetric('murmur_uptime_seconds', 1.0, type: 'gauge'),
    ]);
    $store->put('b', $now, [
        storedMetric('murmur_publish_requests_total', 3, array_reverse($labels)),
        storedMetric('murmur_connections', 4, type: 'gauge'),
        storedMetric('murmur_uptime_seconds', 3.0, type: 'gauge'),
    ]);
    $store->put('old', $now - 100, [storedMetric('murmur_connections', 50, type: 'gauge')]);

    expect(Murmur::metricTotals())->toEqual([
        'murmur_publish_requests_total{status="202",worker="a\\"b"}' => 5,
        'murmur_publish_requests_total{status="404"}' => 9,
        'murmur_connections' => 7,
    ]);
});

it('explains when a metrics store is not configured', function () {
    expect(fn () => Murmur::metrics())->toThrow(MetricsStoreNotConfigured::class);
    expect(fn () => Murmur::metricTotals())->toThrow(MetricsStoreNotConfigured::class);

    foreach (['murmur:status', 'murmur:forget-metrics'] as $command) {
        $this->artisan($command)->expectsOutputToContain('No Murmur metrics store is configured.')->assertExitCode(1);
    }
});

it('prints status figures per node and a totals row', function () {
    config(['murmur.metrics.store' => 'cache', 'murmur.metrics.cache.store' => 'array']);
    $this->freezeTime();
    $store = Murmur::metricsStore();
    $store->put('edge:8080', now()->timestamp - 4, [
        storedMetric('murmur_connections', 2, type: 'gauge'),
        storedMetric('murmur_topics', 3, type: 'gauge'),
        storedMetric('murmur_events_sent_total', 4),
        storedMetric('murmur_errors_total', 5),
        storedMetric('murmur_memory_bytes', 6144, type: 'gauge'),
    ]);
    $store->put('edge:8081', now()->timestamp - 5, [storedMetric('murmur_connections', 9, type: 'gauge')]);

    $this->artisan('murmur:status')->expectsTable(
        ['Node', 'Age (s)', 'Open connections', 'Topics', 'Events sent', 'Errors', 'Memory (bytes)'],
        [
            ['edge:8080', 4, 2, 3, 4, 5, 6144],
            ['edge:8081', 5, '9', '—', '—', '—', '—'],
            ['Totals', '—', 11, 3, 4, 5, 6144],
        ],
    )->assertSuccessful();
});

it('filters both the nodes and totals as JSON', function () {
    config(['murmur.metrics.store' => 'cache', 'murmur.metrics.cache.store' => 'array']);
    $store = Murmur::metricsStore();
    $timestamp = now()->timestamp;
    $samples = [storedMetric('murmur_connections', 2, type: 'gauge')];
    $store->put('0', $timestamp, $samples);
    $store->put('other', $timestamp, [storedMetric('murmur_connections', 12, type: 'gauge')]);

    $output = new BufferedOutput;
    expect(Artisan::call('murmur:status', ['--json' => true, '--node' => '0'], $output))->toBe(0);
    $json = $output->fetch();
    expect(json_decode($json)->nodes)->toBeInstanceOf(stdClass::class);
    expect(json_decode($json, true, flags: JSON_THROW_ON_ERROR))->toBe([
        'nodes' => ['0' => ['recorded_at' => $timestamp, 'samples' => $samples]],
        'totals' => ['murmur_connections' => 2],
    ]);
});

it('prints an explanation when no fresh node matches', function (array $options) {
    config(['murmur.metrics.store' => 'cache', 'murmur.metrics.cache.store' => 'array']);
    Murmur::metricsStore()->put('old', now()->timestamp - 61, []);

    $this->artisan('murmur:status', $options)->expectsOutputToContain('No fresh Murmur metrics snapshots')->assertExitCode(1);
})->with([[[]], [['--json' => true]], [['--node' => 'other']]]);

it('forgets a named node and only stale nodes when the name is omitted', function () {
    config(['murmur.metrics.store' => 'cache', 'murmur.metrics.cache.store' => 'array']);
    $this->freezeTime();
    $store = Murmur::metricsStore();
    $now = now()->timestamp;
    $store->put('remove', $now, []);
    $store->put('keep', $now - 59, []);
    $store->put('stale', $now - 60, []);
    $store->put('older', $now - 90, []);

    $this->artisan('murmur:forget-metrics', ['node' => 'remove'])->assertSuccessful();
    expect(array_keys($store->all()))->toBe(['keep', 'stale', 'older']);
    $this->artisan('murmur:forget-metrics', ['node' => "keep\n"])->assertExitCode(1);
    $this->artisan('murmur:forget-metrics')->assertSuccessful();
    expect(array_keys($store->all()))->toBe(['keep']);
});

it('reads and displays a database snapshot written by a real murmur server after SSE traffic', function () {
    $root = dirname(__DIR__, 2);
    $port = ServerHarness::availablePort();
    $file = tempnam(sys_get_temp_dir(), 'murmur-metrics-db-');
    config([
        'murmur.metrics.store' => 'database',
        'murmur.metrics.database.connection' => 'sqlite',
        'murmur.metrics.database.table' => 'metrics_e2e',
        'database.connections.sqlite' => ['driver' => 'sqlite', 'database' => $file],
    ]);
    $paths = MurmurServiceProvider::pathsToPublish(MurmurServiceProvider::class, 'murmur-migrations');
    $migration = require array_key_first($paths);
    $process = new Process([PHP_BINARY, '-d', 'error_log=', $root.'/vendor/bin/testbench', 'murmur:serve', "--port={$port}"], $root, [
        'MURMUR_SECRET' => config('murmur.secret'),
        'MURMUR_DRIVER' => 'http',
        'MURMUR_HOST' => '127.0.0.1',
        'MURMUR_METRICS_STORE' => 'database',
        'MURMUR_METRICS_DB_CONNECTION' => 'sqlite',
        'MURMUR_METRICS_DB_TABLE' => 'metrics_e2e',
        'MURMUR_METRICS_INTERVAL' => '0.1',
        'MURMUR_NODE' => 'sqlite-e2e',
        'DB_CONNECTION' => 'sqlite',
        'DB_DATABASE' => $file,
    ]);
    $reader = null;

    try {
        $migration->up();
        $process->start();
        waitForMetricsCondition(fn () => str_contains($process->getOutput(), 'Murmur is listening'), $process);
        $reader = SseReader::open("http://127.0.0.1:{$port}/sse?topics=news");
        expect($reader->next()['retry'])->toBe('3000');

        waitForMetricsCondition(function (): bool {
            $node = Murmur::metrics()['sqlite-e2e'] ?? [];
            $samples = array_column($node['samples'] ?? [], 'value', 'name');

            return ($samples['murmur_connections_opened_total'] ?? 0) === 1;
        }, $process);

        $this->artisan('murmur:status')->expectsOutputToContain('sqlite-e2e')->assertSuccessful();
        expect(Murmur::metricTotals()['murmur_connections_opened_total'])->toBe(1);
    } finally {
        $reader?->close();
        $process->stop(0.5);
        DB::disconnect('sqlite');
        unlink($file);
    }
});

it('ignores snapshots dated further ahead than the TTL', function () {
    config(['murmur.metrics.store' => 'cache', 'murmur.metrics.cache.store' => 'array']);
    $this->freezeTime();
    $now = now()->timestamp;
    $store = Murmur::metricsStore();
    $store->put('ahead', $now + 60, []);
    $store->put('forever', PHP_INT_MAX, []);
    $store->put('skewed', $now + 61, []);

    expect(array_keys(Murmur::metrics()))->toBe(['ahead']);

    $this->artisan('murmur:forget-metrics')->expectsOutput('Forgot 2 stale metrics snapshots.')->assertSuccessful();
    expect(array_keys($store->all()))->toBe(['ahead']);
});

it('rejects stored samples whose label names are not identifiers', function (string $label, bool $valid) {
    $json = json_encode(['recorded_at' => 100, 'samples' => [[
        'name' => 'murmur_publish_requests_total', 'type' => 'counter', 'help' => 'Sample.', 'labels' => [$label => '202'], 'value' => 1,
    ]]]);

    expect(Snapshot::decode($json) !== null)->toBe($valid);
})->with([
    ['status', true],
    ['_status_2', true],
    ["x\"} 1\nfake{y", false],
    ['2status', false],
    ['sta tus', false],
    ['', false],
]);
