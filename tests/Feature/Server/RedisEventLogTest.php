<?php

use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Log\RedisEventLog;
use React\Promise\Deferred;
use React\Promise\PromiseInterface;

use function React\Async\await;
use function React\Promise\Timer\sleep;
use function React\Promise\Timer\timeout;

it('delivers entries added by another client', function () {
    [$log, $key] = redisLog();
    $seen = [];
    $log->subscribe(function (Event $e) use (&$seen) {
        $seen[] = $e;
    });
    await(sleep(0.05));
    $id = xadd($key, 'news', 'updated', ['id' => 7]);
    await(sleep(0.1));
    expect($seen)->toHaveCount(1);
    expect((string) $seen[0]->id)->toBe($id);
    expect($seen[0]->topic)->toBe('news');
    expect($seen[0]->name)->toBe('updated');
    expect($seen[0]->data)->toBe(['id' => 7]);
});

it('replays later entries of the requested topics', function () {
    [$log, $key] = redisLog();
    $first = xadd($key, 'news', 'a');
    xadd($key, 'other', 'b');
    $third = xadd($key, 'news', 'c');
    $replay = await($log->since(EventId::tryParse($first), ['news']));
    expect($replay->gap)->toBeFalse();
    expect(array_map(fn (Event $e) => (string) $e->id, $replay->events))->toBe([$third]);
});

it('reports a gap only after the stream has been trimmed', function () {
    [$log, $key] = redisLog(maxLength: 2);
    xadd($key, 'news', 'a');
    expect(await($log->since(new EventId(1, 0), ['news']))->gap)->toBeFalse();
    xadd($key, 'news', 'b', maxLength: 2);
    xadd($key, 'news', 'c', maxLength: 2);
    expect(await($log->since(new EventId(1, 0), ['news']))->gap)->toBeTrue();
});

it('reports no gap on an empty stream', function () {
    [$log] = redisLog();
    $replay = await($log->since(new EventId(1, 0), ['news']));
    expect($replay->gap)->toBeFalse();
    expect($replay->events)->toBe([]);
});

it('delivers entries written while the reader reconnects', function () {
    [$log, $key] = redisLog();
    xadd($key, 'news', 'before-subscribe');
    $client = $this->redisFixtures[$key]['client'];
    $reader = (new ReflectionProperty(RedisEventLog::class, 'reader'))->getValue($log);
    $readerId = await($reader->client('ID'));
    $seen = [];
    $delivered = new Deferred;
    $log->subscribe(function (Event $event) use (&$seen, $delivered) {
        $seen[] = (string) $event->id;
        $delivered->resolve($event);
    });

    $deadline = microtime(true) + 1.0;
    do {
        $info = await($client->client('LIST', 'ID', $readerId));
        if (str_contains($info, 'cmd=xread') && str_contains($info, 'flags=b')) {
            break;
        }

        await(sleep(0.01));
    } while (microtime(true) < $deadline);

    expect($info)->toContain('cmd=xread', 'flags=b');
    expect(await($client->client('KILL', 'ID', $readerId)))->toBe(1);
    await(sleep(0.05));
    $id = xadd($key, 'news', 'during-reconnect');

    await(timeout($delivered->promise(), 3.0));
    await(sleep(0.05));
    expect($seen)->toBe([$id]);
});

it('skips malformed entries and delivers valid entries that follow them', function () {
    [$log, $key] = redisLog();
    $client = $this->redisFixtures[$key]['client'];
    $seen = [];
    $delivered = new Deferred;
    $log->subscribe(function (Event $event) use (&$seen, $delivered) {
        $seen[] = (string) $event->id;

        if ($event->name === 'valid') {
            $delivered->resolve($event);
        }
    });

    await(sleep(0.05));
    await($client->xadd($key, '*', 'topic', 'news', 'name', 'missing-data'));
    await($client->xadd($key, '*', 'topic', 'news', 'name', 'invalid-json', 'data', '{'));
    await($client->xadd($key, '*', 'topic', 'invalid topic', 'name', 'updated', 'data', 'null'));
    await($client->xadd($key, '*', 'topic', 'news', 'name', "invalid\nname", 'data', 'null'));
    $id = xadd($key, 'news', 'valid');

    await(timeout($delivered->promise(), 2.0));
    expect($seen)->toBe([$id]);
    $replay = await($log->since(new EventId(0, 0), ['news', 'invalid topic']));
    expect(array_map(fn (Event $event) => (string) $event->id, $replay->events))->toBe([$id]);
});

it('rejects the replay promise when a redis command fails', function () {
    [$log, $key] = redisLog();
    $client = $this->redisFixtures[$key]['client'];
    await($client->set($key, 'not a stream'));

    $replay = $log->since(new EventId(0, 0), ['news']);

    expect($replay)->toBeInstanceOf(PromiseInterface::class);
    expect(fn () => await($replay))->toThrow(Exception::class, 'WRONGTYPE');
});

it('stops invoking listeners as soon as the log closes', function () {
    [$log, $key] = redisLog();
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

    await(sleep(0.05));
    $id = xadd($key, 'news', 'first');
    await(timeout($closed->promise(), 2.0));
    xadd($key, 'news', 'later');
    $log->close();

    expect($seen)->toBe([$id]);
});
