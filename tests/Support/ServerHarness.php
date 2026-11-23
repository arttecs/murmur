<?php

namespace Arttecs\Murmur\Tests\Support;

use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\Server\Auth\Signature;
use Arttecs\Murmur\Server\Auth\TokenIssuer;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Server\Server;
use Arttecs\Murmur\Server\ServerConfig;
use Psr\Http\Message\ResponseInterface;
use React\Http\Browser;

use function React\Async\await;

/**
 * Run an HTTP server on an ephemeral port for feature tests.
 */
class ServerHarness
{
    /**
     * The harnesses to stop after each test.
     *
     * @var list<ServerHarness>
     */
    private static array $running = [];

    /**
     * Create a harness for a listening server.
     */
    private function __construct(
        private Server $server,
        private ServerConfig $config,
        public readonly EventLog $log,
    ) {}

    /**
     * Start a server with the given configuration overrides and event log.
     *
     * @param  array<string, mixed>  $overrides
     */
    public static function start(array $overrides = [], ?EventLog $log = null): ServerHarness
    {
        $config = new ServerConfig(...$overrides + [
            'secret' => 'test-secret-with-at-least-32-bytes',
            'host' => '127.0.0.1',
            'port' => 0,
        ]);

        $log ??= new MemoryEventLog;
        $server = new Server($config, $log);
        $server->listen();

        return self::$running[] = new self($server, $config, $log);
    }

    /**
     * Get an absolute URL for the given path.
     */
    public function url(string $path): string
    {
        return $this->server->address().$path;
    }

    /**
     * Issue a subscription token for the given topics.
     *
     * @param  list<string>  $topics
     */
    public function token(array $topics, int $ttl = 900): string
    {
        return (new TokenIssuer($this->config->secret))->issue($topics, $ttl);
    }

    /**
     * Publish an event over HTTP.
     *
     * @throws \JsonException
     */
    public function publish(string $topic, string $event, mixed $data = null): ResponseInterface
    {
        return $this->rawPublish(json_encode(compact('topic', 'event', 'data'), JSON_THROW_ON_ERROR));
    }

    /**
     * Post a raw body with a generated or overridden signature.
     */
    public function rawPublish(string $body, ?string $signature = null): ResponseInterface
    {
        $timestamp = time();
        $signature ??= Signature::sign($this->config->secret, $timestamp, $body);

        return await((new Browser)->withRejectErrorResponse(false)->post($this->url('/publish'), [
            'Content-Type' => 'application/json',
            'X-Murmur-Timestamp' => (string) $timestamp,
            'X-Murmur-Signature' => $signature,
        ], $body));
    }

    /**
     * Stop the server and close its connections.
     */
    public function stop(): void
    {
        $this->server->close();
    }

    /**
     * Stop every harness opened during the test.
     */
    public static function stopAll(): void
    {
        foreach (self::$running as $harness) {
            $harness->stop();
        }

        self::$running = [];
    }
}
