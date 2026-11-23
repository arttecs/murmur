<?php

use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SseReader;

it('serves sse subscribers from redis, including replay', function () {
    [$log, $key] = redisLog();
    $h = ServerHarness::start(log: $log);
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
