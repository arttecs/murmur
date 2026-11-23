<?php

use Arttecs\Murmur\MurmurServiceProvider;
use Arttecs\Murmur\Tests\Fixtures\OrderShipped;
use Orchestra\Testbench\Foundation\Application;

require __DIR__.'/../../vendor/autoload.php';

[, $url, $secret] = $argv;
$app = Application::create(options: ['providers' => [MurmurServiceProvider::class]]);
$app['config']->set([
    'broadcasting.default' => 'murmur',
    'broadcasting.connections.murmur' => ['driver' => 'murmur'],
    'murmur.driver' => 'http',
    'murmur.publish_url' => $url,
    'murmur.secret' => $secret,
]);

broadcast(new OrderShipped(orderId: 42, socket: '123.456'));
