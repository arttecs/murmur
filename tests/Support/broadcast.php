<?php

use Arttecs\Murmur\MurmurServiceProvider;
use Arttecs\Murmur\Tests\Fixtures\OrderShipped;
use Illuminate\Support\Facades\Broadcast;
use Orchestra\Testbench\Foundation\Application;

require __DIR__.'/../../vendor/autoload.php';

[, $url, $secret] = $argv;
$app = Application::create(options: ['extra' => ['providers' => [MurmurServiceProvider::class]]]);
$app['config']->set([
    'broadcasting.default' => 'murmur',
    'broadcasting.connections.murmur' => ['driver' => 'murmur'],
    'murmur.driver' => 'http',
    'murmur.publish_url' => $url,
    'murmur.secret' => $secret,
]);

$app['request']->headers->set('X-Socket-ID', $argv[3] ?? '123.456');
Broadcast::event(new OrderShipped(orderId: 42))->toOthers();
