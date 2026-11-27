<?php

namespace Arttecs\Murmur;

use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\Contracts\MetricsStore;
use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Contracts\SubscriptionAuthorizer;
use Arttecs\Murmur\Exceptions\MetricsStoreNotConfigured;
use Arttecs\Murmur\Exceptions\PublishFailed;
use Arttecs\Murmur\Metrics\CacheMetricsStore;
use Arttecs\Murmur\Metrics\DatabaseMetricsStore;
use Arttecs\Murmur\Metrics\MetricsConfig;
use Arttecs\Murmur\Metrics\RedisMetricsStore;
use Arttecs\Murmur\Metrics\Snapshot;
use Arttecs\Murmur\Publishers\HttpPublisher;
use Arttecs\Murmur\Publishers\RedisPublisher;
use Arttecs\Murmur\Server\Auth\Secret;
use Arttecs\Murmur\Server\Auth\TokenIssuer;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Server\Log\RedisEventLog;
use Arttecs\Murmur\Server\Server;
use Arttecs\Murmur\Testing\MurmurFake;
use Closure;
use Illuminate\Contracts\Cache\Factory as CacheFactory;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Contracts\Redis\Factory as RedisFactory;
use Illuminate\Database\ConnectionResolverInterface;
use Illuminate\Http\Client\Factory;
use InvalidArgumentException;
use React\EventLoop\LoopInterface;

/**
 * Publish events, create streams, issue subscription tokens, and read server metrics.
 *
 * @phpstan-import-type Snapshot from MetricsStore
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
     * The in-memory publisher installed for the current test.
     */
    protected ?MurmurFake $fake = null;

    /**
     * The built-in metrics store names.
     */
    protected const METRICS_STORES = ['database', 'redis', 'cache'];

    /**
     * Factories for custom metrics stores.
     *
     * @var array<string, Closure(Application): MetricsStore>
     */
    protected array $metricsFactories = [];

    /**
     * The resolved metrics store for this manager.
     */
    protected ?MetricsStore $metricsStore = null;

    /**
     * The factory used to build a custom authorizer for the server process.
     *
     * @var (Closure(Application): SubscriptionAuthorizer)|null
     */
    protected ?Closure $subscriptionAuthorizerFactory = null;

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
     * Record publications in memory for this application's tests.
     */
    public function fake(): MurmurFake
    {
        $this->fake = new MurmurFake;
        $this->app->instance(Publisher::class, $this->fake);

        return $this->fake;
    }

    /**
     * Resolve the publisher for the configured driver.
     *
     * @throws InvalidArgumentException
     */
    public function publisher(): Publisher
    {
        if ($this->fake !== null) {
            return $this->fake;
        }

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
     * Register a factory for a custom metrics store.
     *
     * @param  Closure(Application): MetricsStore  $factory
     *
     * @throws InvalidArgumentException
     */
    public function extendMetrics(string $store, Closure $factory): void
    {
        if (in_array($store, static::METRICS_STORES, true) || preg_match('/\A[a-z0-9_-]+\z/', $store) !== 1) {
            throw new InvalidArgumentException('Invalid or reserved Murmur metrics store name.');
        }

        $this->metricsFactories[$store] = $factory;
    }

    /**
     * Validate the metrics configuration without connecting to a store.
     *
     * @throws InvalidArgumentException
     */
    public function metricsConfiguration(): ?MetricsConfig
    {
        $store = $this->config->get('murmur.metrics.store');

        if ($store === null || (is_string($store) && trim($store) === '')) {
            return null;
        }

        if (! is_string($store) || (! in_array($store, static::METRICS_STORES, true) && ! isset($this->metricsFactories[$store]))) {
            $name = is_string($store) ? str_replace(["\r", "\n", "\0"], ' ', $store) : get_debug_type($store);

            throw new InvalidArgumentException("Unsupported Murmur metrics store [{$name}].");
        }

        return MetricsConfig::from($this->config, $store);
    }

    /**
     * Resolve the configured metrics store once for this manager.
     *
     * @throws InvalidArgumentException
     */
    public function metricsStore(): ?MetricsStore
    {
        $metrics = $this->metricsConfiguration();

        if ($metrics === null) {
            return null;
        }

        return $this->metricsStore ??= match ($metrics->store) {
            'database' => new DatabaseMetricsStore(
                $this->app->make(ConnectionResolverInterface::class)->connection($this->config->get('murmur.metrics.database.connection')),
                $metrics->table,
            ),
            'redis' => new RedisMetricsStore(
                $this->app->make(RedisFactory::class)->connection($this->config->get('murmur.metrics.redis.connection')),
                $this->config->get('murmur.metrics.redis.key'),
            ),
            'cache' => new CacheMetricsStore(
                $this->app->make(CacheFactory::class)->store($this->config->get('murmur.metrics.cache.store')),
                $this->config->get('murmur.metrics.cache.prefix'),
                (int) min(ceil($metrics->ttl), PHP_INT_MAX >> 1),
            ),
            default => ($this->metricsFactories[$metrics->store])($this->app),
        };
    }

    /**
     * Read fresh snapshots sorted by node name, with values per server process as old as recorded_at.
     *
     * @return array<string, Snapshot>
     *
     * @throws MetricsStoreNotConfigured
     */
    public function metrics(): array
    {
        $store = $this->metricsStore() ?? throw new MetricsStoreNotConfigured;
        $now = now()->timestamp;
        $ttl = (float) $this->config->get('murmur.metrics.ttl');
        $nodes = array_filter($store->all(), fn (array $snapshot): bool => $snapshot['recorded_at'] > $now - $ttl && $snapshot['recorded_at'] <= $now + $ttl);
        ksort($nodes, SORT_STRING);

        return $nodes;
    }

    /**
     * Sum fresh counters and gauges across nodes by Prometheus series name, omitting murmur_uptime_seconds.
     *
     * @return array<string, int|float>
     *
     * @throws MetricsStoreNotConfigured
     */
    public function metricTotals(): array
    {
        return Snapshot::totals($this->metrics());
    }

    /**
     * Publish an event to the given topic.
     *
     * @throws InvalidArgumentException
     * @throws PublishFailed
     */
    public function publish(string $topic, string $event, mixed $data = null, ?string $except = null): void
    {
        if (str_starts_with($event, 'murmur.')) {
            throw new InvalidArgumentException('Event names starting with "murmur." are reserved.');
        }

        $publication = new Event($topic, $event, $data, except: $except);

        $this->app->make(Publisher::class)->publish($publication);
    }

    /**
     * Create a stream for the given topic.
     *
     * @throws InvalidArgumentException
     */
    public function stream(string $topic): Stream
    {
        return new Stream($this, $topic, $this->config->get('murmur.stream_cursor_margin_ms'), $this->app->make(Publisher::class));
    }

    /**
     * Issue a subscription token for the given topics.
     *
     * @param  array<string>  $topics
     *
     * @throws InvalidArgumentException
     */
    public function token(array $topics, ?int $ttl = null): string
    {
        $secret = (string) $this->config->get('murmur.secret');

        if ($secret === '' && $this->fake !== null) {
            $secret = 'murmur-test-secret-for-in-memory-publications';
        }

        $issuer = new TokenIssuer($secret, fn () => now()->timestamp);

        return $issuer->issue($topics, $ttl ?? $this->config->get('murmur.token_ttl'));
    }

    /**
     * Register the factory that builds the server's non-blocking subscription authorizer.
     *
     * @param  Closure(Application): SubscriptionAuthorizer  $factory
     */
    public function authorizeSubscriptionsUsing(Closure $factory): void
    {
        $this->subscriptionAuthorizerFactory = $factory;
    }

    /**
     * Build the subscription authorizer when the server starts.
     */
    public function subscriptionAuthorizer(): ?SubscriptionAuthorizer
    {
        return $this->subscriptionAuthorizerFactory === null
            ? null
            : ($this->subscriptionAuthorizerFactory)($this->app);
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
