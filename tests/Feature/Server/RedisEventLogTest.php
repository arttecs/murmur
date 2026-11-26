<?php

use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Http\HeldConnection;
use Arttecs\Murmur\Server\Log\RedisEventLog;
use Clue\React\Redis\Factory;
use React\EventLoop\StreamSelectLoop;
use React\Promise\Deferred;
use React\Promise\PromiseInterface;

use function React\Async\async;
use function React\Async\await;
use function React\Promise\set_rejection_handler;
use function React\Promise\Timer\sleep;
use function React\Promise\Timer\timeout;

it('reports a gap only after the stream has been trimmed', function () {
    [$log, $key] = redisLog(maxLength: 2);
    xadd($key, 'news', 'a');
    expect(await($log->since(new EventId(1, 0), ['news'], 1000))->gap)->toBeFalse();
    xadd($key, 'news', 'b', maxLength: 2);
    expect(await($log->since(new EventId(1, 0), ['news'], 1000))->gap)->toBeFalse();
    xadd($key, 'news', 'c', maxLength: 2);
    expect(await($log->since(new EventId(1, 0), ['news'], 1000))->gap)->toBeTrue();
});

it('reports trimming even when the configured redis length has been raised', function () {
    [$log, $key] = redisLog(maxLength: 100);
    xadd($key, 'news', 'first');
    xadd($key, 'news', 'second');
    xadd($key, 'news', 'third', maxLength: 2);

    expect(await($log->since(new EventId(1, 0), ['news'], 1000))->gap)->toBeTrue();
});

it('reports no gap on an empty stream', function () {
    [$log] = redisLog();
    $replay = await($log->since(new EventId(1, 0), ['news'], 1000));
    expect($replay->gap)->toBeFalse();
    expect($replay->events)->toBe([]);
});

it('reports a gap when the oldest entry has an unparseable id', function () {
    [$log, $key] = redisLog(maxLength: 2);
    $client = $this->redisFixtures[$key]['client'];
    await($client->xadd($key, '1000000000000000000-0', 'topic', 'news', 'name', 'oversized', 'data', 'null'));

    expect(await($log->since(new EventId(1, 0), ['news'], 1000))->gap)->toBeTrue();
});

it('rejects a redis capacity below one', function (int $capacity) {
    new RedisEventLog('redis://127.0.0.1:6379', 'unused', $capacity);
})->with([0, -1])->throws(InvalidArgumentException::class, 'The event log capacity must be at least 1.');

it('signals lost history before delivering entries written while the reader reconnects', function (?int $maxLength, array $expected) {
    $errorLog = tempnam(sys_get_temp_dir(), 'murmur');
    $previous = ini_set('error_log', $errorLog);

    try {
        [$log, $key] = redisLog();
        xadd($key, 'news', 'before-subscribe');
        $client = $this->redisFixtures[$key]['client'];
        $readerId = redisReaderId($log);
        $seen = [];
        $delivered = new Deferred;
        $log->onGap(fn () => throw new RuntimeException('The gap listener failed.'));
        $log->onGap(function () use (&$seen): void {
            $seen[] = 'gap';
        });
        $log->subscribe(function (Event $event) use (&$seen, $delivered): void {
            $seen[] = $event->name;

            if ($event->name === 'after-reconnect') {
                $delivered->resolve(null);
            }
        });

        waitForRedisReader($client, $readerId);
        expect(await($client->client('KILL', 'ID', $readerId)))->toBe(1);
        waitForRedisRetry($log);
        xadd($key, 'news', 'first-during-reconnect', maxLength: $maxLength);
        xadd($key, 'news', 'second-during-reconnect', maxLength: $maxLength);
        xadd($key, 'news', 'after-reconnect');

        await(timeout($delivered->promise(), 3.0));
        await(sleep(0.05));
        expect($seen)->toBe($expected);
    } finally {
        ini_set('error_log', (string) $previous);
        unlink($errorLog);
    }
})->with([
    'trimmed history' => [1, ['gap', 'second-during-reconnect', 'after-reconnect']],
    'retained history' => [null, ['first-during-reconnect', 'second-during-reconnect', 'after-reconnect']],
]);

it('skips malformed entries and delivers valid entries that follow them', function () {
    [$log, $key] = redisLog();
    $client = $this->redisFixtures[$key]['client'];
    $readerId = redisReaderId($log);
    $seen = [];
    $delivered = new Deferred;
    $log->subscribe(function (Event $event) use (&$seen, $delivered) {
        $seen[] = (string) $event->id;

        if ($event->name === 'valid') {
            $delivered->resolve($event);
        }
    });

    waitForRedisReader($client, $readerId);
    await($client->xadd($key, '*', 'topic', 'news', 'name', 'missing-data'));
    await($client->xadd($key, '*', 'topic', 'news', 'name', 'invalid-json', 'data', '{'));
    await($client->xadd($key, '*', 'topic', 'invalid topic', 'name', 'updated', 'data', 'null'));
    await($client->xadd($key, '*', 'topic', 'news', 'name', "invalid\nname", 'data', 'null'));
    $id = xadd($key, 'news', 'valid');

    await(timeout($delivered->promise(), 2.0));
    expect($seen)->toBe([$id]);
    $replay = await($log->since(new EventId(0, 0), ['news', 'invalid topic'], 1000));
    expect(array_map(fn (Event $event) => (string) $event->id, $replay->events))->toBe([$id]);
});

it('rejects the replay promise when a redis command fails', function () {
    [$log, $key] = redisLog();
    $client = $this->redisFixtures[$key]['client'];
    await($client->set($key, 'not a stream'));

    $replay = $log->since(new EventId(0, 0), ['news'], 1000);

    expect($replay)->toBeInstanceOf(PromiseInterface::class);
    expect(fn () => await($replay))->toThrow(Exception::class, 'WRONGTYPE');
});

it('stops invoking listeners as soon as the log closes', function () {
    [$log, $key] = redisLog();
    $client = $this->redisFixtures[$key]['client'];
    $readerId = redisReaderId($log);
    $seen = [];
    $closed = new Deferred;
    $log->subscribe(function (Event $event) use ($log, &$seen, $closed) {
        $seen[] = (string) $event->id;
        $log->close();
        $closed->resolve(null);
    });
    $log->subscribe(function (Event $event) use (&$seen) {
        $seen[] = (string) $event->id;
    });

    waitForRedisReader($client, $readerId);
    $id = xadd($key, 'news', 'first');
    await(timeout($closed->promise(), 2.0));
    xadd($key, 'news', 'later');
    $log->close();

    expect($seen)->toBe([$id]);
});

it('cancels the reader retry timer when closed during backoff', function () {
    $errorLog = tempnam(sys_get_temp_dir(), 'murmur');
    $previous = ini_set('error_log', $errorLog);

    try {
        $loop = new StreamSelectLoop;
        [$log] = redisLog(loop: $loop);
        $client = (new Factory($loop))->createLazyClient(redisTestUrl());

        try {
            $run = async(function () use ($log, $loop, $client): void {
                $readerId = redisReaderId($log);
                $log->subscribe(fn () => null);
                waitForRedisReader($client, $readerId, $loop);
                expect(await($client->client('KILL', 'ID', $readerId)))->toBe(1);
                $retry = waitForRedisRetry($log, $loop);
                $timers = (new ReflectionProperty(StreamSelectLoop::class, 'timers'))->getValue($loop);
                expect($timers->contains($retry))->toBeTrue();

                $log->close();

                expect($timers->contains($retry))->toBeFalse();
            })();
            $run->then($loop->stop(...), $loop->stop(...));
            $loop->run();
            await($run);
        } finally {
            $log->close();
            $client->close();
        }
    } finally {
        ini_set('error_log', (string) $previous);
        unlink($errorLog);
    }
});

it('closes the reader while its initial cursor is unresolved', function () {
    [$log, $key] = redisLog();
    xadd($key, 'news', 'already-in-stream');
    redisReaderId($log);
    $cursor = new ReflectionProperty(RedisEventLog::class, 'cursor');
    $retry = new ReflectionProperty(RedisEventLog::class, 'retry');
    $seen = [];
    $log->subscribe(function (Event $event) use (&$seen): void {
        $seen[] = $event;
    });
    expect($cursor->getValue($log))->toBeNull();

    $log->close();
    xadd($key, 'news', 'after-close');

    expect(fn () => redisReaderId($log))->toThrow(RuntimeException::class, 'Connection closed');
    expect($cursor->getValue($log))->toBeNull();
    expect($retry->getValue($log))->toBeNull();
    expect($seen)->toBe([]);
});

it('rejects replay requests made after closing', function () {
    [$log] = redisLog();
    $log->close();

    $replay = $log->since(new EventId(0, 0), ['news'], 1000);

    expect($replay)->toBeInstanceOf(PromiseInterface::class);
    expect(fn () => await($replay))->toThrow(RuntimeException::class, 'Connection closed');
});

it('shares one blocked xread connection between subscribers', function () {
    [$log, $key] = redisLog();
    $client = $this->redisFixtures[$key]['client'];
    preg_match_all('/^id=(\d+) /m', await($client->client('LIST')), $existingClients);
    $readerId = redisReaderId($log);
    $first = new Deferred;
    $second = new Deferred;
    $deliveries = [0, 0];
    $log->subscribe(function (Event $event) use ($first, &$deliveries): void {
        $deliveries[0]++;
        $first->resolve($event);
    });
    waitForRedisReader($client, $readerId);

    $log->subscribe(function (Event $event) use ($second, &$deliveries): void {
        $deliveries[1]++;
        $second->resolve($event);
    });
    $id = xadd($key, 'news', 'shared');

    expect((string) await(timeout($first->promise(), 2.0))->id)->toBe($id);
    expect((string) await(timeout($second->promise(), 2.0))->id)->toBe($id);
    waitForRedisReader($client, $readerId);
    expect($deliveries)->toBe([1, 1]);
    preg_match_all('/^id=(\d+) [^\r\n]*\bflags=b [^\r\n]*\bcmd=xread\b/m', await($client->client('LIST')), $blockedReaders);
    expect(array_values(array_diff($blockedReaders[1], $existingClients[1])))->toBe([(string) $readerId]);
});

it('cleans later redis fixtures even if deleting an earlier fixture fails', function () {
    [$observer, $key, $url] = redisStream();
    $factory = new Factory;
    $disconnected = $factory->createLazyClient($url);
    $disconnected->close();
    $client = $factory->createLazyClient($url);
    $log = new RedisEventLog($url, $key);
    xadd($key, 'news', 'first');

    try {
        expect(fn () => cleanRedisFixtures([
            $key.':failed' => ['log' => null, 'client' => $disconnected],
            $key => ['log' => $log, 'client' => $client],
        ]))->toThrow(RuntimeException::class, 'Connection closed');

        expect(await($observer->exists($key)))->toBe(0);
        expect(fn () => await($client->ping()))->toThrow(RuntimeException::class, 'Connection closed');
        expect(fn () => await($log->since(new EventId(0, 0), ['news'], 1000)))->toThrow(RuntimeException::class, 'Connection closed');
    } finally {
        $log->close();
        $client->close();
    }
});

it('continues delivering a redis batch and subsequent reads after a listener throws', function () {
    $loop = new StreamSelectLoop;
    [$log, $key] = redisLog(loop: $loop);
    $client = (new Factory($loop))->createLazyClient(redisTestUrl());
    $error = new RuntimeException('The first Redis listener failed.');
    $seen = [];
    $unhandled = [];
    \React\Async\delay(1.01);
    $errorLog = tempnam(sys_get_temp_dir(), 'murmur');
    $previousErrorLog = ini_set('error_log', $errorLog);
    $previousHandler = set_rejection_handler(function (Throwable $reason) use (&$unhandled): void {
        $unhandled[] = $reason;
    });

    try {
        $run = async(function () use ($log, $key, $loop, $client, $error, &$seen): void {
            $readerId = redisReaderId($log);
            $batch = new Deferred;
            $later = new Deferred;
            $log->subscribe(function (Event $event) use ($error): void {
                if ($event->name === 'first') {
                    throw $error;
                }
            });
            $log->subscribe(function (Event $event) use (&$seen, $batch, $later): void {
                $seen[] = $event->name;

                match ($event->name) {
                    'second' => $batch->resolve(null),
                    'later' => $later->resolve(null),
                    default => null,
                };
            });

            waitForRedisReader($client, $readerId, $loop);
            await($client->multi());
            await($client->xadd($key, '*', 'topic', 'news', 'name', 'first', 'data', 'null'));
            await($client->xadd($key, '*', 'topic', 'news', 'name', 'second', 'data', 'null'));
            await($client->exec());
            await(timeout($batch->promise(), 2.0, $loop));

            await($client->xadd($key, '*', 'topic', 'news', 'name', 'later', 'data', 'null'));
            await(timeout($later->promise(), 2.0, $loop));
        })();
        $run->then($loop->stop(...), $loop->stop(...));

        $loop->run();

        expect($seen)->toBe(['first', 'second', 'later']);
        expect(file_get_contents($errorLog))->toContain('The first Redis listener failed.');
        expect($unhandled)->toBe([]);
        await($run);
    } finally {
        $log->close();
        $client->close();
        set_rejection_handler($previousHandler);
        ini_set('error_log', (string) $previousErrorLog);
        unlink($errorLog);
    }
});

it('pages through interleaved redis topics and limits matching replay events', function () {
    [$log, $key] = redisLog();
    $client = $this->redisFixtures[$key]['client'];
    $first = xadd($key, 'news', 'first');
    $writes = [];

    for ($i = 1; $i <= 502; $i++) {
        $topic = in_array($i, [2, 502], true) ? 'news' : 'other';
        $writes[] = $client->xadd($key, '*', 'topic', $topic, 'name', "entry-{$i}", 'data', 'null');
    }

    await(React\Promise\all($writes));
    $limited = await($log->since(EventId::tryParse($first), ['news'], 1));
    expect($limited->gap)->toBeTrue()->and($limited->events)->toBe([]);
    $rangeCalls = function () use ($client): int {
        preg_match('/cmdstat_xrange:calls=(\d+)/', await($client->info('commandstats')), $matches);

        return (int) ($matches[1] ?? 0);
    };

    $before = $rangeCalls();
    $replay = await($log->since(EventId::tryParse($first), ['news'], 2));
    expect($rangeCalls() - $before)->toBeGreaterThanOrEqual(2);
    expect($replay->gap)->toBeFalse();
    expect(array_map(fn (Event $event) => $event->name, $replay->events))->toBe(['entry-2', 'entry-502']);
});

it('skips oversized redis payloads in live delivery and replay', function () {
    [$log, $key] = redisLog(maxPayloadBytes: 8);
    $client = $this->redisFixtures[$key]['client'];
    $first = xadd($key, 'news', 'first');
    $readerId = redisReaderId($log);
    $seen = [];
    $delivered = new Deferred;

    $log->subscribe(function (Event $event) use (&$seen, $delivered): void {
        $seen[] = $event->name;

        if ($event->name === 'fits') {
            $delivered->resolve(null);
        }
    });
    waitForRedisReader($client, $readerId);
    xadd($key, 'news', 'oversized', '1234567');
    xadd($key, 'news', 'fits', '123456');
    await(timeout($delivered->promise(), 2.0));

    expect($seen)->toBe(['fits']);
    $replay = await($log->since(EventId::tryParse($first), ['news'], 10));
    expect(array_map(fn (Event $event) => $event->name, $replay->events))->toBe(['fits']);
});

it('delivers the next redis event through a held connection when its cursor is ahead', function (bool $empty) {
    [$log, $key] = redisLog();
    $tail = $empty ? null : xadd($key, 'news', 'old');
    $cursor = new EventId(999999999999999999, 0);
    [, $output, $stream] = fakeConnection();
    $connection = new HeldConnection($stream, cursor: $cursor);
    $log->subscribe($connection->sendEvent(...));
    await(timeout($log->ready(), 2.0));
    $replay = await($log->since($cursor, ['news'], 1000));

    expect($replay->tail === null ? null : (string) $replay->tail)->toBe($tail);
    $connection->release($replay);

    $delivered = new Deferred;
    $log->subscribe(fn (Event $event) => $delivered->resolve($event));
    $id = xadd($key, 'news', 'live');
    await(timeout($delivered->promise(), 2.0));

    expect($output())->toContain("id: {$id}\nevent: live\n");
})->with(['empty log' => [true], 'nonempty log' => [false]]);
