<?php

use Arttecs\Murmur\Metrics\CacheMetricsStore;
use Arttecs\Murmur\Metrics\DatabaseMetricsStore;
use Arttecs\Murmur\Metrics\RedisMetricsStore;
use Arttecs\Murmur\MurmurServiceProvider;
use Illuminate\Contracts\Redis\Factory as RedisFactory;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

it('replaces and removes snapshots per node and ignores malformed entries', function (string $driver) {
    switch ($driver) {
        case 'database':
            config([
                'database.connections.metrics-test' => ['driver' => 'sqlite', 'database' => ':memory:'],
                'murmur.metrics.database.connection' => 'metrics-test',
                'murmur.metrics.database.table' => 'custom_metrics',
            ]);
            expect(Schema::connection('metrics-test')->hasTable('custom_metrics'))->toBeFalse();
            $paths = MurmurServiceProvider::pathsToPublish(MurmurServiceProvider::class, 'murmur-migrations');
            expect($paths)->toHaveCount(1);
            $migration = require array_key_first($paths);
            $migration->up();
            $database = DB::connection('metrics-test');
            $database->table('custom_metrics')->insert([
                'node' => 'foreign', 'recorded_at' => 100, 'samples' => '{"wrong":[]}',
            ]);
            $store = new DatabaseMetricsStore($database, 'custom_metrics');
            break;
        case 'redis':
            [$client, $key, $url] = redisStream();
            $prefix = 'murmur:test:';
            $name = substr($key, strlen($prefix));
            config([
                'database.redis.client' => getenv('REDIS_CLIENT') ?: 'phpredis',
                'database.redis.metrics-test' => ['url' => $url, 'prefix' => $prefix],
                'database.redis.options.prefix' => $prefix,
            ]);
            $redis = app(RedisFactory::class)->connection('metrics-test');
            $redis->hset($name, 'foreign', '{"recorded_at":100,"samples":[null]}');
            expect(React\Async\await($client->hget($key, 'foreign')))->not->toBeNull();
            $store = new RedisMetricsStore($redis, $name);
            break;
        case 'cache':
            $cache = Cache::store('array');
            $cache->put('test:index', ['foreign']);
            $cache->put('test:node:foreign', 'not-json');
            $store = new CacheMetricsStore($cache, 'test:', 60);
            break;
    }

    $sample = [
        'name' => 'murmur_publish_requests_total', 'type' => 'counter',
        'help' => 'Publish responses.', 'labels' => ['status' => '202'], 'value' => 2,
    ];

    try {
        expect($store->all())->toBe([]);
        $store->put('alpha', 100, [$sample]);
        $store->put('node:2', 101, []);
        expect($store->all())->toEqual([
            'alpha' => ['recorded_at' => 100, 'samples' => [$sample]],
            'node:2' => ['recorded_at' => 101, 'samples' => []],
        ]);

        $store->put('alpha', 102, []);
        expect($store->all()['alpha'])->toBe(['recorded_at' => 102, 'samples' => []]);

        $store->forget('alpha');
        $store->forget('missing');
        expect($store->all())->toBe(['node:2' => ['recorded_at' => 101, 'samples' => []]]);
    } finally {
        if ($driver === 'database') {
            $migration->down();
            expect(Schema::connection('metrics-test')->hasTable('custom_metrics'))->toBeFalse();
        }
    }
})->with(['database', 'redis', 'cache']);

it('expires cache entries independently and retains live nodes in the index', function () {
    $store = new CacheMetricsStore(Cache::store('array'), 'test:', 3);

    $store->put('old', now()->timestamp, []);
    $this->travel(2)->seconds();
    $store->put('new', now()->timestamp, []);
    $this->travel(2)->seconds();

    expect(array_keys($store->all()))->toBe(['new']);
    $store->put('latest', now()->timestamp, []);
    expect(array_keys($store->all()))->toBe(['new', 'latest']);

    $this->travel(4)->seconds();
    expect(Cache::store('array')->get('test:index'))->toBeNull();
});
