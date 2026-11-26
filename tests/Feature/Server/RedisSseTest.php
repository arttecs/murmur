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
    $r->start(replaying: true);
    expect($r->next()['event'])->toBe('b');
    $r->attached();
    xadd($key, 'news', 'c');
    expect($r->next()['event'])->toBe('c');
});

it('does not expose the publish endpoint with a redis log', function () {
    [$log] = redisLog();
    $h = ServerHarness::start(log: $log);
    expect($h->rawPublish('{"topic":"news","event":"x"}')->getStatusCode())->toBe(404);
});

it('does not advance heartbeats past events the redis live reader has not dispatched', function () {
    $errorLog = tempnam(sys_get_temp_dir(), 'murmur');
    $previous = ini_set('error_log', $errorLog);

    try {
        [$log, $key] = redisLog();
        $client = $this->redisFixtures[$key]['client'];
        $readerId = redisReaderId($log);
        $h = ServerHarness::start(['heartbeatSeconds' => 0.2], $log);
        $r = SseReader::open($h->url('/sse?topics=news'));
        $r->start();
        $warmup = xadd($key, 'news', 'warmup');
        expect($r->next()['id'])->toBe($warmup);

        waitForRedisReader($client, $readerId);
        expect(await($client->client('KILL', 'ID', $readerId)))->toBe(1);
        waitForRedisRetry($log);
        $pending = xadd($key, 'news', 'pending');
        expect($r->next())->toMatchArray(['event' => 'murmur.heartbeat', 'id' => $warmup]);

        $deadline = hrtime(true) + 3_000_000_000;
        do {
            $frame = $r->next();
            if ($frame['event'] === 'murmur.heartbeat') {
                expect($frame['id'])->toBe($warmup);
            }
        } while ($frame['event'] === 'murmur.heartbeat' && hrtime(true) < $deadline);

        expect($frame)->toMatchArray(['event' => 'pending', 'id' => $pending]);
        expect($r->next())->toMatchArray(['event' => 'murmur.heartbeat', 'id' => $pending]);
    } finally {
        ini_set('error_log', (string) $previous);
        unlink($errorLog);
    }
});

it('sends a gap to an already connected subscriber after redis trims unread events', function () {
    $errorLog = tempnam(sys_get_temp_dir(), 'murmur');
    $previous = ini_set('error_log', $errorLog);

    try {
        [$log, $key] = redisLog();
        $client = $this->redisFixtures[$key]['client'];
        xadd($key, 'news', 'before-subscribe');
        $readerId = redisReaderId($log);
        $h = ServerHarness::start(log: $log);
        $r = SseReader::open($h->url('/sse?topics=news'));
        expect($r->start()['retry'])->toBe('3000');

        waitForRedisReader($client, $readerId);
        expect(await($client->client('KILL', 'ID', $readerId)))->toBe(1);
        waitForRedisRetry($log);
        xadd($key, 'news', 'trimmed', maxLength: 1);
        $retained = xadd($key, 'news', 'retained', maxLength: 1);

        $gap = $r->next(3.0);
        expect($gap['event'])->toBe('murmur.gap')->and($gap['data'])->toBe('{}')->and($gap['id'])->toBeNull();
        $frame = $r->next();
        expect($frame['event'])->toBe('retained')->and($frame['id'])->toBe($retained);
        $live = xadd($key, 'news', 'live');
        expect($r->next()['id'])->toBe($live);
    } finally {
        ini_set('error_log', (string) $previous);
        unlink($errorLog);
    }
});

it('does not repeat an event that the replay already delivered', function () {
    $errorLog = tempnam(sys_get_temp_dir(), 'murmur');
    $previous = ini_set('error_log', $errorLog);

    try {
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

        expect($r->start(replaying: true)['retry'])->toBe('3000');
        $frame = $r->next();
        expect($frame['event'])->toBe('b')->and($frame['id'])->toBe($replayed);
        expect($r->attached()['id'])->toBe($replayed);

        $live = xadd($key, 'news', 'c');
        $frame = $r->next(3.0);
        expect($frame['event'])->toBe('c')->and($frame['id'])->toBe($live);
    } finally {
        ini_set('error_log', (string) $previous);
        unlink($errorLog);
    }
});

it('binds only after redis is ready and delivers an immediate publish to a new subscriber', function () {
    [$log, $key] = redisLog();
    $server = new Server(
        new ServerConfig('test-secret-with-at-least-32-bytes', port: ServerHarness::availablePort()),
        $log,
    );

    try {
        $listening = $server->listen();
        expect($server->address(...))->toThrow(LogicException::class, 'not listening');
        await(React\Promise\Timer\timeout($listening, 2.0));

        $r = SseReader::open($server->address().'/sse?topics=news');
        expect($r->start(replaying: true)['retry'])->toBe('3000');
        $id = xadd($key, 'news', 'immediate');
        $frames = [$r->next(), $r->next()];
        $byEvent = array_column($frames, 'id', 'event');
        expect($byEvent)->toHaveKeys(['murmur.attached', 'immediate']);
        expect($byEvent['immediate'])->toBe($id);
        expect(fn () => $r->next(0.05))->toThrow(RuntimeException::class, 'Timed out');
    } finally {
        $server->close();
    }
});

it('suppresses retained fresh events even when the redis live reader sees them after attachment', function (bool $withCursor) {
    $errorLog = tempnam(sys_get_temp_dir(), 'murmur');
    $previous = ini_set('error_log', $errorLog);

    try {
        [$log, $key] = redisLog();
        $client = $this->redisFixtures[$key]['client'];
        $cursor = xadd($key, 'old', 'cursor');
        $readerId = redisReaderId($log);
        $h = ServerHarness::start(log: $log);
        waitForRedisReader($client, $readerId);

        expect(await($client->client('KILL', 'ID', $readerId)))->toBe(1);
        waitForRedisRetry($log);
        $history = xadd($key, 'old', 'retained-old');
        $tail = xadd($key, 'new', 'retained-fresh');
        $query = $withCursor ? "topics=old,new&lastEventId={$cursor}" : 'topics=new';
        $r = SseReader::open($h->url('/sse?'.$query.'&fresh=new'));
        $r->start(replaying: true);

        if ($withCursor) {
            expect($r->next()['id'])->toBe($history);
        }

        expect($r->attached()['id'])->toBe($tail);
        expect(fn () => $r->next(1.2))->toThrow(RuntimeException::class, 'Timed out');
        $fresh = xadd($key, 'new', 'live-fresh');
        expect($r->next()['id'])->toBe($fresh);
    } finally {
        ini_set('error_log', (string) $previous);
        unlink($errorLog);
    }
})->with([false, true]);
