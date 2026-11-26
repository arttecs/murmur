<?php

use Arttecs\Murmur\Contracts\MetricsStore;
use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\Metrics\CacheMetricsStore;
use Arttecs\Murmur\Metrics\DatabaseMetricsStore;
use Arttecs\Murmur\Metrics\RedisMetricsStore;
use Arttecs\Murmur\Server\Metrics;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\ServiceProvider;

it('leaves metrics disabled without validating unused settings', function (mixed $name) {
    config(['murmur.metrics.store' => $name, 'murmur.metrics.interval' => 0]);

    expect(Murmur::metricsStore())->toBeNull();
})->with([null, '', '  ']);

it('rejects invalid metrics configuration when a store is resolved', function (string $key, mixed $value, string $message) {
    config(['murmur.metrics.store' => 'cache', "murmur.metrics.{$key}" => $value]);

    expect(fn () => Murmur::metricsStore())->toThrow(InvalidArgumentException::class, $message);
})->with([
    ['interval', 0, 'metrics.interval'],
    ['interval', '12junk', 'metrics.interval'],
    ['interval', INF, 'metrics.interval'],
    ['ttl', -1, 'metrics.ttl'],
    ['ttl', NAN, 'metrics.ttl'],
    ['ttl', 15, 'greater than'],
    ['timeout', 0, 'metrics.timeout'],
    ['timeout', '1e400', 'metrics.timeout'],
    ['node', 'node name', 'metrics.node'],
    ['node', "name\n", 'metrics.node'],
    ['node', str_repeat('a', 129), 'metrics.node'],
    ['only', ['murmur_typo'], 'metrics.only'],
    ['only', 'murmur_connections', 'metrics.only'],
    ['only', [0], 'metrics.only'],
    ['database.table', "metrics\n", 'metrics.database.table'],
    ['database.table', 'other.metrics', 'metrics.database.table'],
    ['store', 'missing', 'Unsupported Murmur metrics store [missing].'],
]);

it('resolves built-in stores once with their configured services', function (string $name, string $class) {
    config([
        'murmur.metrics.store' => $name,
        'murmur.metrics.database.connection' => 'sqlite',
        'murmur.metrics.cache.store' => 'array',
        'murmur.metrics.cache.prefix' => 'configured:',
        'murmur.metrics.only' => ['murmur_publish_requests_total', 'murmur_connections'],
    ]);

    $store = Murmur::metricsStore();
    expect($store)->toBeInstanceOf($class)->toBe(Murmur::metricsStore());

    if ($name === 'cache') {
        $store->put('node-1', 1, []);
        expect(Cache::store('array')->get('configured:index'))->toBe(['node-1']);
    }
})->with([
    ['database', DatabaseMetricsStore::class],
    ['cache', CacheMetricsStore::class],
    ['redis', RedisMetricsStore::class],
]);

it('exposes the known names even for counters without a series yet', function () {
    expect(Metrics::names())->toContain('murmur_publish_requests_total', 'murmur_connections_rejected_total', 'murmur_replay_failures_total', 'murmur_uptime_seconds');
});

it('resolves a custom metrics store registered from a provider boot method once', function () {
    config(['murmur.metrics.store' => 'custom_1-store']);
    $store = new CacheMetricsStore(Cache::store('array'), 'custom:', 60);
    $this->app->instance(MetricsStore::class, $store);
    $this->app->register(new class($this->app) extends ServiceProvider
    {
        public function boot(): void
        {
            Murmur::extendMetrics('custom_1-store', function (Application $app): MetricsStore {
                $app['config']->set('metrics_factory_calls', $app['config']->get('metrics_factory_calls', 0) + 1);

                return $app->make(MetricsStore::class);
            });
        }
    });

    expect(Murmur::metricsStore())->toBe($store)
        ->and(Murmur::metricsStore())->toBe($store)
        ->and(config('metrics_factory_calls'))->toBe(1);
});

it('rejects malformed and reserved custom metrics store names', function (string $name) {
    expect(fn () => Murmur::extendMetrics($name, fn () => throw new LogicException))->toThrow(InvalidArgumentException::class);
})->with(['database', 'redis', 'cache', '', 'Upper', 'one two', 'one/two', "name\n"]);
