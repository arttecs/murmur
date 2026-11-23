<?php

use Arttecs\Murmur\Console\Commands\ServeCommand;
use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Exceptions\PublishFailed;
use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\Publishers\RedisPublisher;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SseReader;

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
        expect($seen[0]->topic)->toBe('news');
        expect($seen[0]->name)->toBe('updated');
        expect($seen[0]->data)->toEqual((object) ['id' => 7]);
    } finally {
        await($client->del('app_'.$key));
    }
});

it('rejects an unknown driver', function () {
    config(['murmur.driver' => 'carrier-pigeon']);
    app(Publisher::class);
})->throws(InvalidArgumentException::class);

it('reports a payload that cannot be encoded as a publish failure', function () {
    [, $key, $url] = redisStream();
    config(['murmur.driver' => 'redis', 'murmur.redis.stream' => $key, 'database.redis.default' => ['url' => $url], 'database.redis.options.prefix' => '']);

    try {
        Murmur::publish('news', 'updated', ['value' => INF]);
    } catch (PublishFailed $exception) {
        expect($exception->getPrevious())->toBeInstanceOf(JsonException::class);

        return;
    }

    $this->fail('The publish did not fail.');
});

it('reports a redis error reply as a publish failure', function () {
    [$client, $key, $url] = redisStream();
    config(['murmur.driver' => 'redis', 'murmur.redis.stream' => $key, 'database.redis.default' => ['url' => $url], 'database.redis.options.prefix' => '']);
    await($client->set($key, 'not a stream'));

    try {
        Murmur::publish('news', 'updated');
    } catch (PublishFailed $exception) {
        expect($exception->getPrevious()->getMessage())->toContain('WRONGTYPE');

        return;
    }

    $this->fail('The publish did not fail.');
});

it('rejects an encoded redis payload above the configured limit', function () {
    [$client, $key, $url] = redisStream();
    config([
        'murmur.driver' => 'redis',
        'murmur.redis.stream' => $key,
        'murmur.server.max_payload_bytes' => 8,
        'database.redis.default' => ['url' => $url],
        'database.redis.options.prefix' => '',
    ]);

    Murmur::publish('news', 'fits', '123456');
    expect(fn () => Murmur::publish('news', 'oversized', '1234567'))->toThrow(PublishFailed::class);
    expect(await($client->xlen($key)))->toBe(1);
});

it('delivers the same data frame through http and redis', function (string $json) {
    [, $key, $url] = redisStream();
    config([
        'murmur.driver' => 'redis',
        'murmur.redis.url' => $url,
        'murmur.redis.stream' => $key,
        'database.redis.default' => ['url' => $url],
        'database.redis.options.prefix' => '',
    ]);

    $redis = ServerHarness::start(log: app(ServeCommand::class)->makeLog());
    $http = ServerHarness::start();
    $redisSubscriber = SseReader::open($redis->url('/sse?topics=news'));
    $httpSubscriber = SseReader::open($http->url('/sse?topics=news'));
    $redisSubscriber->next();
    $httpSubscriber->next();

    expect($http->rawPublish('{"topic":"news","event":"updated","data":'.$json.'}')->getStatusCode())->toBe(202);
    Murmur::publish('news', 'updated', json_decode($json, flags: JSON_THROW_ON_ERROR));

    $expected = '{"topic":"news","data":'.$json.'}';
    expect($httpSubscriber->next()['data'])->toBe($expected);
    expect($redisSubscriber->next()['data'])->toBe($expected);
})->with(['{"meta":{}}', '{"list":[]}', '{"list":[{},[],{"nested":[]}]}', 'null', '42', '"hello"']);
