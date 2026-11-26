<?php

use Arttecs\Murmur\MurmurServiceProvider;
use Arttecs\Murmur\Tests\Support\MetricsTestServiceProvider;
use Orchestra\Testbench\Foundation\Application as Testbench;
use Symfony\Component\Console\Input\ArgvInput;

require dirname(__DIR__, 2).'/vendor/autoload.php';

if (getenv('MURMUR_TEST_METRICS_SESSION')) {
    posix_setsid();
    putenv('MURMUR_TEST_METRICS_SESSION');
}

if ($entry = getenv('MURMUR_TEST_ARTISAN_BINARY')) {
    define('ARTISAN_BINARY', $entry);
}

$app = Testbench::create(options: ['extra' => ['providers' => [MurmurServiceProvider::class, MetricsTestServiceProvider::class]]]);

exit($app->handleCommand(new ArgvInput));
