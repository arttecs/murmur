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

        if ($data === [] && $this->name() !== 'test_disabling_authorization_leaves_the_route_unregistered') {
            return;
        }

        [$variable, $value] = $data ?: ['MURMUR_AUTH_ENABLED', 'false'];
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

    public function test_disabling_authorization_leaves_the_route_unregistered(): void
    {
        $this->assertFalse($this->app['router']->has('murmur.auth'));
    }

    public function test_fills_missing_settings_in_published_config_groups(): void
    {
        config(['murmur' => [
            'server' => ['allowed_origins' => []],
            'auth' => ['middleware' => []],
            'redis' => ['stream' => 'app-events'],
        ]]);
        (new MurmurServiceProvider($this->app))->register();

        $this->assertSame([], config('murmur.server.allowed_origins'));
        $this->assertSame([], config('murmur.auth.middleware'));
        $this->assertSame('app-events', config('murmur.redis.stream'));
        $this->assertSame(5.0, config('murmur.server.shutdown_grace'));
        $this->assertSame(5000, config('murmur.redis.block_ms'));
        $this->assertTrue(config('murmur.auth.enabled'));
    }

    public static function settings(): array
    {
        return [
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
            ['MURMUR_ALLOWED_ORIGINS', 'a, b,,c', 'server.allowed_origins', ['a', 'b', 'c']],
            ['MURMUR_SIGNATURE_TOLERANCE', '45', 'server.signature_tolerance', 45],
            ['MURMUR_SHUTDOWN_GRACE', '7.5', 'server.shutdown_grace', 7.5],
            ['MURMUR_PUBLIC_URL', 'https://events.test', 'public_url', 'https://events.test'],
            ['MURMUR_PUBLISH_URL', 'http://internal.test', 'publish_url', 'http://internal.test'],
            ['MURMUR_TOKEN_TTL', '600', 'token_ttl', 600],
            ['MURMUR_STREAM_CURSOR_MARGIN_MS', '500', 'stream_cursor_margin_ms', 500],
            ['MURMUR_HTTP_TIMEOUT', '1.5', 'http.timeout', 1.5],
            ['MURMUR_HTTP_CONNECT_TIMEOUT', '0.5', 'http.connect_timeout', 0.5],
            ['MURMUR_AUTH_PATH', 'events/auth', 'auth.path', 'events/auth'],
            ['MURMUR_AUTH_MIDDLEWARE', 'a, b,,c', 'auth.middleware', ['a', 'b', 'c']],
            ['MURMUR_AUTH_ENABLED', 'false', 'auth.enabled', false],
            ['MURMUR_REDIS_CONNECTION', 'events', 'redis.connection', 'events'],
            ['MURMUR_REDIS_URL', 'redis://redis.test:6380', 'redis.url', 'redis://redis.test:6380'],
            ['MURMUR_REDIS_STREAM', 'my-app:events', 'redis.stream', 'my-app:events'],
            ['MURMUR_REDIS_QUERY_TIMEOUT', '0.5', 'redis.query_timeout', 0.5],
            ['MURMUR_REDIS_BLOCK_MS', '2000', 'redis.block_ms', 2000],
        ];
    }
}
