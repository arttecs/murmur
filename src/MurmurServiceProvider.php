<?php

namespace Arttecs\Murmur;

use Arttecs\Murmur\Broadcasting\MurmurBroadcaster;
use Arttecs\Murmur\Console\Commands\ForgetMetricsCommand;
use Arttecs\Murmur\Console\Commands\InstallCommand;
use Arttecs\Murmur\Console\Commands\RecordMetricsCommand;
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

        $config = $this->app['config'];

        if (! $config->has('broadcasting.connections.murmur')) {
            $config->set('broadcasting.connections.murmur', ['driver' => 'murmur']);
        }

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
        $overrides = (array) $config->get('murmur', []);
        $merged = array_replace($defaults, $overrides);

        foreach (['server', 'http', 'auth', 'redis', 'metrics'] as $group) {
            $merged[$group] = array_replace($defaults[$group], (array) ($overrides[$group] ?? []));
        }

        foreach (['database', 'redis', 'cache'] as $store) {
            $merged['metrics'][$store] = array_replace($defaults['metrics'][$store], (array) ($merged['metrics'][$store] ?? []));
        }

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
            $this->publishesMigrations([
                __DIR__.'/../database/migrations/2026_11_23_000000_create_murmur_metrics_table.php' => $this->app->databasePath('migrations/2026_11_23_000000_create_murmur_metrics_table.php'),
            ], 'murmur-migrations');

            $this->commands([InstallCommand::class, ServeCommand::class, RecordMetricsCommand::class, StatusCommand::class, ForgetMetricsCommand::class]);
        }
    }
}
