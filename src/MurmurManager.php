<?php

namespace Arttecs\Murmur;

use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Exceptions\MetricsUnavailable;
use Arttecs\Murmur\Exceptions\PublishFailed;
use Arttecs\Murmur\Publishers\HttpPublisher;
use Arttecs\Murmur\Publishers\RedisPublisher;
use Arttecs\Murmur\Server\Auth\Secret;
use Arttecs\Murmur\Server\Auth\TokenIssuer;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Server\Log\RedisEventLog;
use Closure;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Contracts\Redis\Factory as RedisFactory;
use Illuminate\Http\Client\Factory;
use Illuminate\Http\Client\HttpClientException;
use InvalidArgumentException;
use stdClass;

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
     * Read the metrics of one server process at the configured publish URL.
     *
     * @return list<array{name: string, type: 'counter'|'gauge', help: string, labels: array<string, string>, value: int|float}>
     *
     * @throws MetricsUnavailable
     */
    public function metrics(): array
    {
        try {
            $request = $this->app->make(Factory::class)
                ->acceptJson()
                ->timeout($this->config->get('murmur.http.timeout'))
                ->connectTimeout($this->config->get('murmur.http.connect_timeout'))
                ->withoutRedirecting();

            if (($token = $this->config->get('murmur.server.metrics.token')) !== null) {
                $request->withToken($token);
            }

            $response = $request->get(rtrim($this->config->get('murmur.publish_url'), '/').'/metrics');
            $status = $response->status();
            $body = $status === 200 ? $response->body() : '';
        } catch (HttpClientException $exception) {
            throw new MetricsUnavailable('The metrics request to the Murmur server failed.', previous: $exception);
        }

        if ($status !== 200) {
            throw new MetricsUnavailable("The Murmur server returned HTTP status {$status} for the metrics request.");
        }

        return $this->decodeMetrics($body);
    }

    /**
     * Decode a metrics response and validate its sample and label types.
     *
     * @return list<array{name: string, type: 'counter'|'gauge', help: string, labels: array<string, string>, value: int|float}>
     *
     * @throws MetricsUnavailable
     */
    protected function decodeMetrics(string $body): array
    {
        $samples = json_decode($body)->metrics ?? null;

        if (! is_array($samples)) {
            throw new MetricsUnavailable('The Murmur server returned malformed metrics.');
        }

        $metrics = [];

        foreach ($samples as $sample) {
            $labels = $sample->labels ?? null;
            $value = $sample->value ?? null;

            if (! is_string($sample->name ?? null)
                || ! in_array($sample->type ?? null, ['counter', 'gauge'], true)
                || ! is_string($sample->help ?? null)
                || ($labels !== [] && ! $labels instanceof stdClass)
                || (! is_int($value) && ! is_float($value))
                || ! is_finite((float) $value)) {
                throw new MetricsUnavailable('The Murmur server returned malformed metrics.');
            }

            $labels = (array) $labels;

            foreach ($labels as $label => $text) {
                if (! is_string($label) || ! is_string($text)) {
                    throw new MetricsUnavailable('The Murmur server returned malformed metrics.');
                }
            }

            $metrics[] = [
                'name' => $sample->name,
                'type' => $sample->type,
                'help' => $sample->help,
                'labels' => $labels,
                'value' => $value,
            ];
        }

        return $metrics;
    }

    /**
     * Get the public base URL of the Murmur server.
     */
    public function publicUrl(): string
    {
        return rtrim($this->config->get('murmur.public_url'), '/');
    }
}
