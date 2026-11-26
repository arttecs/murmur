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
use React\Promise\PromiseInterface;
use React\Socket\ConnectionInterface;
use React\Socket\Connector;
use React\Socket\SocketServer;
use ReflectionProperty;

use function React\Async\await;
use function React\Async\delay;

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
    protected static array $running = [];

    /**
     * Create a harness for a listening server.
     */
    protected function __construct(
        protected Server $server,
        protected ServerConfig $config,
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
            'port' => self::availablePort(),
        ]);

        $log ??= new MemoryEventLog;
        $server = new Server($config, $log);
        await($server->listen());

        return self::$running[] = new self($server, $config, $log);
    }

    /**
     * Find an available port for a server configuration.
     */
    public static function availablePort(): int
    {
        $socket = new SocketServer('127.0.0.1:0');

        try {
            return parse_url($socket->getAddress(), PHP_URL_PORT);
        } finally {
            $socket->close();
        }
    }

    /**
     * Get an absolute URL for the given path.
     */
    public function url(string $path): string
    {
        return $this->server->address().$path;
    }

    /**
     * Count the TCP sockets still tracked by the server.
     */
    public function trackedSocketCount(): int
    {
        return count((new ReflectionProperty(Server::class, 'clients'))->getValue($this->server));
    }

    /**
     * Connect a raw TCP client to the server.
     */
    public function connect(): ConnectionInterface
    {
        return await((new Connector)->connect(substr($this->server->address(), strlen('http://'))));
    }

    /**
     * Open a raw subscription without reading its response.
     */
    public function subscribeWithoutReading(string $path): ConnectionInterface
    {
        $client = $this->connect();
        $client->pause();
        $client->write("GET {$path} HTTP/1.1\r\nHost: localhost\r\n\r\n");

        return $client;
    }

    /**
     * Poll health until it shows the expected connection count or the deadline passes.
     */
    public function pollHealth(int $connections, float $timeout = 1.0): ResponseInterface
    {
        $browser = (new Browser)->withTimeout($timeout);
        $deadline = hrtime(true) + (int) ($timeout * 1_000_000_000);

        do {
            $response = await($browser->get($this->url('/health'), ['Connection' => 'close']));

            if (json_decode((string) $response->getBody(), true)['connections'] === $connections) {
                break;
            }

            delay(0.01);
        } while (hrtime(true) < $deadline);

        return $response;
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
     * Drain the server until all sockets close or the grace period expires.
     */
    public function shutdown(?float $graceSeconds = null): PromiseInterface
    {
        return $this->server->shutdown($graceSeconds);
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
