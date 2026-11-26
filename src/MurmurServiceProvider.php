<?php

namespace Arttecs\Murmur;

use Arttecs\Murmur\Broadcasting\MurmurBroadcaster;
use Arttecs\Murmur\Console\Commands\ServeCommand;
use Arttecs\Murmur\Console\Commands\StatusCommand;
use Arttecs\Murmur\Contracts\Publisher;
use Illuminate\Broadcasting\BroadcastManager;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Contracts\Foundation\CachesConfiguration;
use Illuminate\Support\ServiceProvider;

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
        $this->mergeMurmurConfig();

        $this->app->singleton(Publisher::class, fn (Application $app): Publisher => $app->make(MurmurManager::class)->publisher());

        $this->app->singleton(MurmurManager::class);
    }

    /**
     * Fill missing settings in published configuration while preserving list overrides.
     */
    protected function mergeMurmurConfig(): void
    {
        if ($this->app instanceof CachesConfiguration && $this->app->configurationIsCached()) {
            return;
        }

        $config = $this->app['config'];
        $defaults = require __DIR__.'/../config/murmur.php';
        $overrides = $config->get('murmur', []);
        $merged = array_replace($defaults, $overrides);

        foreach (['server', 'http', 'auth', 'redis'] as $group) {
            $merged[$group] = array_replace($defaults[$group], $overrides[$group] ?? []);
        }

        $merged['server']['metrics'] = array_replace($defaults['server']['metrics'], $overrides['server']['metrics'] ?? []);

        $config->set('murmur', $merged);
    }

    /**
     * Get the Redis stream key with Laravel's configured prefix.
     */
    public static function fullStreamKey(): string
    {
        return config('database.redis.options.prefix').config('murmur.redis.stream');
    }

    /**
     * Register the broadcast driver, authorization route, configuration, and commands.
     */
    public function boot(): void
    {
        $this->callAfterResolving(BroadcastManager::class, function (BroadcastManager $manager): void {
            $manager->extend('murmur', fn (Application $app) => $app->make(MurmurBroadcaster::class));
        });

        if ($this->app['config']->get('murmur.auth.enabled')) {
            $this->loadRoutesFrom(__DIR__.'/../routes/murmur.php');
        }

        $this->publishes([
            __DIR__.'/../config/murmur.php' => $this->app->configPath('murmur.php'),
        ], 'murmur-config');

        if ($this->app->runningInConsole()) {
            $this->commands([ServeCommand::class, StatusCommand::class]);
        }
    }
}
