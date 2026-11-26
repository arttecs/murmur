<?php

namespace Arttecs\Murmur;

use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Exceptions\PublishFailed;
use Arttecs\Murmur\Publishers\HttpPublisher;
use Arttecs\Murmur\Publishers\RedisPublisher;
use Arttecs\Murmur\Server\Auth\Secret;
use Arttecs\Murmur\Server\Auth\TokenIssuer;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Server\Log\RedisEventLog;
use Arttecs\Murmur\Server\Server;
use Closure;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Contracts\Redis\Factory as RedisFactory;
use Illuminate\Http\Client\Factory;
use InvalidArgumentException;
use React\EventLoop\LoopInterface;

/**
 * Publish events, create streams and issue subscription tokens.
 */
class MurmurManager
{
    /**
     * The registered application and server factories for custom drivers.
     *
     * @var array<string, array{publisher: Closure(Application): Publisher, eventLog: Closure(Application): EventLog}>
     */
    protected array $drivers = [];

    /**
     * The callbacks to run when the Artisan command creates the server.
     *
     * @var list<Closure(Server, LoopInterface): void>
     */
    protected array $servingCallbacks = [];

    /**
     * Create a manager without resolving the configured driver or signing secret.
     */
    public function __construct(protected Application $app, protected Repository $config) {}

    /**
     * Register the publisher and event log factories for a custom driver.
     *
     * @param  Closure(Application): Publisher  $publisher
     * @param  Closure(Application): EventLog  $eventLog
     *
     * @throws InvalidArgumentException
     */
    public function extend(string $driver, Closure $publisher, Closure $eventLog): void
    {
        if (in_array($driver, ['http', 'redis'], true) || preg_match('/\A[a-z0-9_-]+\z/', $driver) !== 1) {
            throw new InvalidArgumentException("Invalid or reserved Murmur driver [{$driver}].");
        }

        $this->drivers[$driver] = ['publisher' => $publisher, 'eventLog' => $eventLog];
    }

    /**
     * Resolve the publisher for the configured driver.
     *
     * @throws InvalidArgumentException
     */
    public function publisher(): Publisher
    {
        $driver = $this->config->get('murmur.driver');

        if (isset($this->drivers[$driver])) {
            return ($this->drivers[$driver]['publisher'])($this->app);
        }

        $secret = (string) $this->config->get('murmur.secret');

        if (in_array($driver, ['http', 'redis'], true)) {
            Secret::assertValid($secret);
        }

        return match ($driver) {
            'http' => new HttpPublisher(
                $this->app->make(Factory::class),
                $this->config->get('murmur.publish_url'),
                $secret,
                $this->config->get('murmur.http.timeout'),
                $this->config->get('murmur.http.connect_timeout'),
            ),
            'redis' => new RedisPublisher(
                $this->app->make(RedisFactory::class),
                $this->config->get('murmur.redis.connection'),
                MurmurServiceProvider::fullStreamKey(),
                $this->config->get('murmur.server.log_size'),
                $this->config->get('murmur.server.max_payload_bytes'),
            ),
            default => throw new InvalidArgumentException("Unsupported Murmur publisher driver [{$driver}]."),
        };
    }

    /**
     * Resolve the event log for the configured server driver.
     *
     * @throws InvalidArgumentException
     */
    public function eventLog(): EventLog
    {
        $driver = $this->config->get('murmur.driver');

        if (isset($this->drivers[$driver])) {
            return ($this->drivers[$driver]['eventLog'])($this->app);
        }

        return match ($driver) {
            'http' => new MemoryEventLog((int) $this->config->get('murmur.server.log_size')),
            'redis' => new RedisEventLog(
                $this->config->get('murmur.redis.url'),
                MurmurServiceProvider::fullStreamKey(),
                $this->config->get('murmur.server.log_size'),
                maxPayloadBytes: $this->config->get('murmur.server.max_payload_bytes'),
                queryTimeoutSeconds: $this->config->get('murmur.redis.query_timeout'),
                blockMs: $this->config->get('murmur.redis.block_ms'),
            ),
            default => throw new InvalidArgumentException("Unsupported Murmur server driver [{$driver}]."),
        };
    }

    /**
     * Publish an event to the given topic.
     *
     * @throws InvalidArgumentException
     * @throws PublishFailed
     */
    public function publish(string $topic, string $event, mixed $data = null): void
    {
        if (str_starts_with($event, 'murmur.')) {
            throw new InvalidArgumentException('Event names starting with "murmur." are reserved.');
        }

        $this->app->make(Publisher::class)->publish(new Event($topic, $event, $data));
    }

    /**
     * Create a stream for the given topic.
     */
    public function stream(string $topic): Stream
    {
        return new Stream($this, $topic, $this->config->get('murmur.stream_cursor_margin_ms'), $this->app->make(Publisher::class));
    }

    /**
     * Issue a subscription token for the given topics.
     *
     * @param  array<string>  $topics
     */
    public function token(array $topics, ?int $ttl = null): string
    {
        $issuer = new TokenIssuer((string) $this->config->get('murmur.secret'), fn () => now()->timestamp);

        return $issuer->issue($topics, $ttl ?? $this->config->get('murmur.token_ttl'));
    }

    /**
     * Register a callback that runs when the Artisan command creates the server.
     *
     * @param  Closure(Server, LoopInterface): void  $callback
     */
    public function serving(Closure $callback): void
    {
        $this->servingCallbacks[] = $callback;
    }

    /**
     * Run the registered callbacks for a server that is about to listen.
     */
    public function callServingCallbacks(Server $server, LoopInterface $loop): void
    {
        foreach ($this->servingCallbacks as $callback) {
            $callback($server, $loop);
        }
    }

    /**
     * Get the public base URL of the Murmur server.
     */
    public function publicUrl(): string
    {
        return rtrim($this->config->get('murmur.public_url'), '/');
    }
}
