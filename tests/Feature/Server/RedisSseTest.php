<?php

use Arttecs\Murmur\Server\Server;
use Arttecs\Murmur\Server\ServerConfig;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SseReader;

use function React\Async\await;

it('serves sse subscribers from redis, including replay', function () {
    [$log, $key] = redisLog();
    $client = $this->redisFixtures[$key]['client'];
    $readerId = redisReaderId($log);
    $h = ServerHarness::start(log: $log);
    waitForRedisReader($client, $readerId);

    $first = xadd($key, 'news', 'a');
    xadd($key, 'news', 'b');
    $r = SseReader::open($h->url('/sse?topics=news'), ['Last-Event-ID' => $first]);
    $r->next();
    expect($r->next()['event'])->toBe('b');
    xadd($key, 'news', 'c');
    expect($r->next()['event'])->toBe('c');
});

it('does not expose the publish endpoint with a redis log', function () {
    [$log] = redisLog();
    $h = ServerHarness::start(log: $log);
    expect($h->rawPublish('{"topic":"news","event":"x"}')->getStatusCode())->toBe(404);
});

it('does not repeat an event that the replay already delivered', function () {
    [$log, $key] = redisLog();
    $client = $this->redisFixtures[$key]['client'];
    $first = xadd($key, 'news', 'a');
    $readerId = redisReaderId($log);
    $h = ServerHarness::start(log: $log);
    waitForRedisReader($client, $readerId);

    expect(await($client->client('KILL', 'ID', $readerId)))->toBe(1);
    waitForRedisRetry($log);
    $replayed = xadd($key, 'news', 'b');
    $r = SseReader::open($h->url('/sse?topics=news'), ['Last-Event-ID' => $first]);

    expect($r->next()['retry'])->toBe('3000');
    $frame = $r->next();
    expect($frame['event'])->toBe('b')->and($frame['id'])->toBe($replayed);

    $live = xadd($key, 'news', 'c');
    $frame = $r->next(3.0);
    expect($frame['event'])->toBe('c')->and($frame['id'])->toBe($live);
});

it('binds only after redis is ready and delivers an immediate publish to a new subscriber', function () {
    [$log, $key] = redisLog();
    $server = new Server(
        new ServerConfig('test-secret-with-at-least-32-bytes', port: 0),
        $log,
    );

    try {
        $listening = $server->listen();
        expect($server->address(...))->toThrow(LogicException::class, 'not listening');
        await(React\Promise\Timer\timeout($listening, 2.0));

        $r = SseReader::open($server->address().'/sse?topics=news');
        $id = xadd($key, 'news', 'immediate');
        expect($r->next()['retry'])->toBe('3000');
        expect($r->next()['id'])->toBe($id);
    } finally {
        $server->close();
    }
});
