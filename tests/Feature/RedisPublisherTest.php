<?php

use Arttecs\Murmur\Console\Commands\ServeCommand;
use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\Publishers\RedisPublisher;
use Arttecs\Murmur\Server\Event;

use function React\Async\await;
use function React\Async\delay;

it('selects the redis publisher from config', function () {
    config(['murmur.driver' => 'redis']);
    expect(app(Publisher::class))->toBeInstanceOf(RedisPublisher::class);
});

it('writes entries the server log can read, despite the laravel key prefix', function () {
    [$client, $key, $url] = redisStream();
    config([
        'murmur.driver' => 'redis',
        'murmur.redis.connection' => 'default',
        'murmur.redis.url' => $url,
        'murmur.redis.stream' => $key,
        'database.redis.default' => ['url' => $url],
        'database.redis.options.prefix' => 'app_',
    ]);
    $log = app(ServeCommand::class)->makeLog();
    $this->redisFixtures[$key]['log'] = $log;
    $readerId = redisReaderId($log);
    $seen = [];
    $log->subscribe(function (Event $e) use (&$seen) {
        $seen[] = $e;
    });

    try {
        waitForRedisReader($client, $readerId);
        Murmur::publish('news', 'updated', ['id' => 7]);
        delay(0.1);
        expect($seen)->toHaveCount(1);
        expect($seen[0]->data)->toBe(['id' => 7]);
    } finally {
        await($client->del('app_'.$key));
    }
});

it('rejects an unknown driver', function () {
    config(['murmur.driver' => 'carrier-pigeon']);
    app(Publisher::class);
})->throws(InvalidArgumentException::class);
