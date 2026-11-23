<?php

namespace Arttecs\Murmur;

use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Publishers\HttpPublisher;
use Arttecs\Murmur\Server\Auth\Secret;
use Illuminate\Contracts\Foundation\Application;
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
                default => throw new InvalidArgumentException("Unsupported Murmur publisher driver [{$driver}]."),
            };
        });

        $this->app->singleton(MurmurManager::class);
    }

    /**
     * Register the package configuration for publishing.
     */
    public function boot(): void
    {
        $this->publishes([
            __DIR__.'/../config/murmur.php' => $this->app->configPath('murmur.php'),
        ], 'murmur-config');
    }
}
