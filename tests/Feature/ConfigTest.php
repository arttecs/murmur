<?php

namespace Arttecs\Murmur\Tests\Feature;

use Arttecs\Murmur\MurmurServiceProvider;
use Arttecs\Murmur\Tests\TestCase;
use PHPUnit\Framework\Attributes\DataProvider;

class ConfigTest extends TestCase
{
    protected function defineEnvironment($app): void
    {
        parent::defineEnvironment($app);

        $data = $this->providedData();

        if ($data === []) {
            return;
        }

        [$variable, $value] = $data;
        $previousEnv = $_ENV;
        $previousServer = $_SERVER;

        try {
            $_ENV[$variable] = $_SERVER[$variable] = $value;
            $app['config']->set('murmur', require __DIR__.'/../../config/murmur.php');
        } finally {
            $_ENV = $previousEnv;
            $_SERVER = $previousServer;
        }
    }

    #[DataProvider('settings')]
    public function test_resolves_environment_settings_with_the_right_type(string $variable, string $value, string $key, mixed $expected): void
    {
        $this->assertSame($expected, config('murmur.'.$key));
    }

    public function test_fills_missing_settings_in_published_config_groups(): void
    {
        config(['murmur' => [
            'server' => ['allowed_origins' => []],
            'redis' => ['stream' => 'app-events'],
            'metrics' => [
                'only' => [],
                'database' => ['table' => 'app_metrics'],
                'redis' => ['key' => 'app:metrics'],
                'cache' => ['prefix' => 'app:metrics:'],
            ],
        ]]);
        (new MurmurServiceProvider($this->app))->register();

        $this->assertSame([], config('murmur.server.allowed_origins'));
        $this->assertSame(['enabled' => false, 'buffer' => 1000, 'batch' => 100, 'interval' => 1.0], config('murmur.lifecycle'));
        $this->assertSame('app-events', config('murmur.redis.stream'));
        $this->assertSame(5.0, config('murmur.server.shutdown_grace'));
        $this->assertSame(2.0, config('murmur.server.shutdown_spread'));
        $this->assertSame(100, config('murmur.server.loop_lag_threshold_ms'));
        $this->assertSame(5000, config('murmur.redis.block_ms'));
        $this->assertNull(config('murmur.metrics.store'));
        $this->assertSame(15.0, config('murmur.metrics.interval'));
        $this->assertSame(60, config('murmur.metrics.ttl'));
        $this->assertSame(10.0, config('murmur.metrics.timeout'));
        $this->assertSame([], config('murmur.metrics.only'));
        $this->assertSame(['connection' => null, 'table' => 'app_metrics'], config('murmur.metrics.database'));
        $this->assertSame(['connection' => 'default', 'key' => 'app:metrics'], config('murmur.metrics.redis'));
        $this->assertSame(['store' => null, 'prefix' => 'app:metrics:'], config('murmur.metrics.cache'));
    }

    public static function settings(): array
    {
        return [
            ['MURMUR_LIFECYCLE_EVENTS', 'true', 'lifecycle.enabled', true],
            ['MURMUR_LIFECYCLE_BUFFER', '10', 'lifecycle.buffer', '10'],
            ['MURMUR_LIFECYCLE_BATCH', '2', 'lifecycle.batch', '2'],
            ['MURMUR_LIFECYCLE_INTERVAL', '0.25', 'lifecycle.interval', '0.25'],
            ['MURMUR_DRIVER', 'custom', 'driver', 'custom'],
            ['MURMUR_SECRET', 'different-secret-with-at-least-32-bytes', 'secret', 'different-secret-with-at-least-32-bytes'],
            ['MURMUR_HOST', '0.0.0.0', 'server.host', '0.0.0.0'],
            ['MURMUR_PORT', '8081', 'server.port', 8081],
            ['MURMUR_HEARTBEAT', '15.5', 'server.heartbeat', 15.5],
            ['MURMUR_RETRY_MS', '4000', 'server.retry_ms', 4000],
            ['MURMUR_LOG_SIZE', '20000', 'server.log_size', 20000],
            ['MURMUR_MAX_CONNECTIONS', '1200', 'server.max_connections', 1200],
            ['MURMUR_MAX_SOCKETS', '1400', 'server.max_sockets', 1400],
            ['MURMUR_REQUEST_TIMEOUT', '8.5', 'server.request_timeout', 8.5],
            ['MURMUR_MAX_REPLAY_EVENTS', '500', 'server.max_replay_events', 500],
            ['MURMUR_MAX_TOPICS_PER_CONNECTION', '25', 'server.max_topics_per_connection', 25],
            ['MURMUR_MAX_PAYLOAD_BYTES', '32000', 'server.max_payload_bytes', 32000],
            ['MURMUR_MAX_BUFFER_BYTES', '65536', 'server.max_buffer_bytes', 65536],
            ['MURMUR_ALLOWED_ORIGINS', 'https://a.test, https://b.test,,https://c.test', 'server.allowed_origins', ['https://a.test', 'https://b.test', 'https://c.test']],
            ['MURMUR_ALLOWED_ORIGINS', ' ', 'server.allowed_origins', []],
            ['MURMUR_SIGNATURE_TOLERANCE', '45', 'server.signature_tolerance', 45],
            ['MURMUR_LOOP_LAG_THRESHOLD_MS', '25', 'server.loop_lag_threshold_ms', 25],
            ['MURMUR_SHUTDOWN_SPREAD', '0.25', 'server.shutdown_spread', 0.25],
            ['MURMUR_SHUTDOWN_GRACE', '7.5', 'server.shutdown_grace', 7.5],
            ['MURMUR_LOG_CHANNEL', '', 'server.log_channel', null],
            ['MURMUR_LOG_CHANNEL', 'stderr', 'server.log_channel', 'stderr'],
            ['MURMUR_PUBLIC_URL', 'https://events.test', 'public_url', 'https://events.test'],
            ['MURMUR_PUBLISH_URL', 'http://internal.test', 'publish_url', 'http://internal.test'],
            ['MURMUR_TOKEN_TTL', '600', 'token_ttl', 600],
            ['MURMUR_STREAM_CURSOR_MARGIN_MS', '500', 'stream_cursor_margin_ms', 500],
            ['MURMUR_HTTP_TIMEOUT', '1.5', 'http.timeout', 1.5],
            ['MURMUR_HTTP_CONNECT_TIMEOUT', '0.5', 'http.connect_timeout', 0.5],
            ['MURMUR_REDIS_CONNECTION', 'events', 'redis.connection', 'events'],
            ['MURMUR_REDIS_URL', 'redis://redis.test:6380', 'redis.url', 'redis://redis.test:6380'],
            ['MURMUR_REDIS_STREAM', 'my-app:events', 'redis.stream', 'my-app:events'],
            ['MURMUR_REDIS_QUERY_TIMEOUT', '0.5', 'redis.query_timeout', 0.5],
            ['MURMUR_REDIS_BLOCK_MS', '2000', 'redis.block_ms', 2000],
            ['MURMUR_METRICS_STORE', 'redis', 'metrics.store', 'redis'],
            ['MURMUR_METRICS_STORE', '  ', 'metrics.store', null],
            ['MURMUR_METRICS_INTERVAL', '1.5', 'metrics.interval', 1.5],
            ['MURMUR_METRICS_INTERVAL', ' ', 'metrics.interval', 15.0],
            ['MURMUR_METRICS_TTL', '120', 'metrics.ttl', 120],
            ['MURMUR_METRICS_TIMEOUT', '2.5', 'metrics.timeout', 2.5],
            ['MURMUR_NODE', 'edge-1:8080', 'metrics.node', 'edge-1:8080'],
            ['MURMUR_NODE', '', 'metrics.node', null],
            ['MURMUR_METRICS_ONLY', 'a, b,,c', 'metrics.only', ['a', 'b', 'c']],
            ['MURMUR_METRICS_ONLY', '', 'metrics.only', []],
            ['MURMUR_METRICS_DB_CONNECTION', 'events', 'metrics.database.connection', 'events'],
            ['MURMUR_METRICS_DB_CONNECTION', '', 'metrics.database.connection', null],
            ['MURMUR_METRICS_DB_TABLE', 'events_metrics', 'metrics.database.table', 'events_metrics'],
            ['MURMUR_METRICS_REDIS_CONNECTION', 'events', 'metrics.redis.connection', 'events'],
            ['MURMUR_METRICS_REDIS_KEY', 'events:metrics', 'metrics.redis.key', 'events:metrics'],
            ['MURMUR_METRICS_CACHE_STORE', 'events', 'metrics.cache.store', 'events'],
            ['MURMUR_METRICS_CACHE_STORE', '', 'metrics.cache.store', null],
            ['MURMUR_METRICS_CACHE_PREFIX', 'events:metrics:', 'metrics.cache.prefix', 'events:metrics:'],
        ];
    }
}
