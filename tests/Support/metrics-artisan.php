<?php

use Arttecs\Murmur\MurmurServiceProvider;
use Arttecs\Murmur\Tests\Support\MetricsTestServiceProvider;
use Orchestra\Testbench\Foundation\Application as Testbench;
use Symfony\Component\Console\Input\ArgvInput;

require dirname(__DIR__, 2).'/vendor/autoload.php';

$app = Testbench::create(options: ['extra' => ['providers' => [MurmurServiceProvider::class, MetricsTestServiceProvider::class]]]);

exit($app->handleCommand(new ArgvInput));
