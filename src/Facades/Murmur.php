<?php

namespace Arttecs\Murmur\Facades;

use Arttecs\Murmur\MurmurManager;
use Illuminate\Support\Facades\Facade;

/**
 * Access the Murmur manager through Laravel's facade.
 *
 * @method static \Arttecs\Murmur\Testing\MurmurFake fake()
 * @method static void extend(string $driver, \Closure $publisher, \Closure $eventLog)
 * @method static void extendMetrics(string $store, \Closure $factory)
 * @method static \Arttecs\Murmur\Metrics\MetricsConfig|null metricsConfiguration()
 * @method static \Arttecs\Murmur\Contracts\MetricsStore|null metricsStore()
 * @method static array<string, array{recorded_at: int, samples: list<array>}> metrics()
 * @method static array<string, int|float> metricTotals()
 * @method static void publish(string $topic, string $event, mixed $data = null, ?string $except = null)
 * @method static \Arttecs\Murmur\Stream stream(string $topic)
 * @method static string token(array<string> $topics, ?int $ttl = null)
 * @method static void authorizeSubscriptionsUsing(\Closure $factory)
 * @method static void serving(\Closure $callback)
 * @method static string publicUrl()
 *
 * @see MurmurManager
 */
class Murmur extends Facade
{
    /**
     * Get the container binding for the facade.
     *
     * @return class-string<MurmurManager>
     */
    protected static function getFacadeAccessor(): string
    {
        return MurmurManager::class;
    }
}
