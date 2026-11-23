<?php

namespace Arttecs\Murmur;

use Arttecs\Murmur\Broadcasting\MurmurBroadcaster;
use Arttecs\Murmur\Console\Commands\ServeCommand;
use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Publishers\HttpPublisher;
use Arttecs\Murmur\Publishers\RedisPublisher;
use Arttecs\Murmur\Server\Auth\Secret;
use Illuminate\Broadcasting\BroadcastManager;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Contracts\Redis\Factory as RedisFactory;
use Illuminate\Http\Client\Factory;
use Illuminate\Support\ServiceProvider;
use InvalidArgumentException;

/**
 * Register the Laravel services and configuration for Murmur.
 */
class MurmurServiceProvider extends ServiceProvider
{
    /**
     * Merge the package configuration and register its services.
     */
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__.'/../config/murmur.php', 'murmur');

        $this->app->singleton(Publisher::class, function (Application $app): Publisher {
            $config = $app['config'];
            $driver = $config->get('murmur.driver');
            $secret = (string) $config->get('murmur.secret');
            Secret::assertValid($secret);

            return match ($driver) {
                'http' => new HttpPublisher(
                    $app->make(Factory::class),
                    $config->get('murmur.publish_url'),
                    $secret,
                    $config->get('murmur.http.timeout'),
                    $config->get('murmur.http.connect_timeout'),
                ),
                'redis' => new RedisPublisher(
                    $app->make(RedisFactory::class),
                    $config->get('murmur.redis.connection'),
                    static::fullStreamKey(),
                    $config->get('murmur.server.log_size'),
                ),
                default => throw new InvalidArgumentException("Unsupported Murmur publisher driver [{$driver}]."),
            };
        });

        $this->app->singleton(MurmurManager::class);
    }

    /**
     * Get the Redis stream key with Laravel's configured prefix.
     */
    public static function fullStreamKey(): string
    {
        return config('database.redis.options.prefix').config('murmur.redis.stream');
    }

    /**
     * Register the broadcast driver, authorization route, configuration, and command.
     */
    public function boot(): void
    {
        $this->callAfterResolving(BroadcastManager::class, function (BroadcastManager $manager): void {
            $manager->extend('murmur', fn (Application $app) => $app->make(MurmurBroadcaster::class));
        });

        $this->loadRoutesFrom(__DIR__.'/../routes/murmur.php');

        $this->publishes([
            __DIR__.'/../config/murmur.php' => $this->app->configPath('murmur.php'),
        ], 'murmur-config');

        if ($this->app->runningInConsole()) {
            $this->commands([ServeCommand::class]);
        }
    }
}
