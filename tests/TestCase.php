<?php

namespace Arttecs\Murmur\Tests;

use Arttecs\Murmur\MurmurServiceProvider;
use Illuminate\Foundation\Application;
use Orchestra\Testbench\TestCase as OrchestraTestCase;

/**
 * Boot a Laravel application with the Murmur package.
 */
class TestCase extends OrchestraTestCase
{
    /**
     * Register the Murmur service provider.
     *
     * @param  Application  $app
     * @return list<class-string>
     */
    protected function getPackageProviders($app): array
    {
        return [MurmurServiceProvider::class];
    }

    /**
     * Configure the shared secret used by the tests.
     *
     * @param  Application  $app
     */
    protected function defineEnvironment($app): void
    {
        $app['config']->set('murmur.secret', 'test-secret-with-at-least-32-bytes');
    }
}
