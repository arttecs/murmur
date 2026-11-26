<?php

use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SseReader;

use function React\Async\await;

/**
 * Open the selected log and return a publisher that yields the assigned ID.
 */
function attachTestLog(string $driver): array
{
    if ($driver === 'redis') {
        [$log, $key] = redisLog();

        $client = test()->target->redisFixtures[$key]['client'];

        return [$log, function (string $topic, string $name, ?string $except = null) use ($client, $key): string {
            $fields = $except === null ? [] : ['except', $except];

            return await($client->xadd($key, '*', 'topic', $topic, 'name', $name, 'data', 'null', ...$fields));
        }];
    }

    $log = new MemoryEventLog;

    return [$log, fn (string $topic, string $name, ?string $except = null): string => (string) $log->append(new Event($topic, $name, except: $except))->id];
}

it('writes the literal handshake and replay before live delivery', function (string $driver, bool $cursor, bool $fresh, string $history) {
    [$log, $publish] = attachTestLog($driver);
    $h = ServerHarness::start(['maxReplayEvents' => 1], $log);
    $id = str_repeat('a', 32);
    $after = '0-0';
    $tail = null;
    $replayed = null;

    if ($history !== 'empty') {
        $after = $publish('old', 'seen');
        $replayed = $publish('old', 'replayed');

        if ($history === 'over limit') {
            $publish('old', 'over-limit');
        }

        $publish('old', 'excluded', $id);
        $tail = $publish($fresh ? 'new' : 'unsubscribed', 'tail');
    } else {
        $after = '999999999999999999-0';
    }

    $path = '/sse?topics=old,new&connection='.$id.($fresh ? '&fresh=new' : '');
    $r = SseReader::open($h->url($path), $cursor ? ['Last-Event-ID' => $after] : []);
    expect($r->nextRaw())->toBe('retry: 3000');
    expect($r->nextRaw())->toBe("event: murmur.connected\ndata: {\"connection_id\":\"{$id}\",\"protocol\":1}");

    if ($cursor && $history === 'over limit') {
        expect($r->nextRaw())->toBe("event: murmur.gap\ndata: {}");
    }

    if ($cursor && $history === 'within limit') {
        expect($r->nextRaw())->toBe("id: {$replayed}\nevent: replayed\ndata: {\"topic\":\"old\",\"data\":null}");
    }

    $expected = $tail === null ? '' : "id: {$tail}\n";
    expect($r->nextRaw())->toBe($expected."event: murmur.attached\ndata: {}");
    $live = $publish('new', 'live');
    expect($r->nextRaw())->toBe("id: {$live}\nevent: live\ndata: {\"topic\":\"new\",\"data\":null}");

    if ($cursor && $history === 'over limit') {
        $resumed = SseReader::open($h->url('/sse?topics=old,new&connection='.$id), ['Last-Event-ID' => $tail]);
        $resumed->next();
        $resumed->next();
        expect($resumed->next()['id'])->toBe($live);
        expect($resumed->next()['event'])->toBe('murmur.attached');
    }
})->with(['memory', 'redis'], [false, true], [false, true], ['empty', 'within limit', 'over limit']);

it('resumes a newly joined topic from the acknowledged join after disconnecting', function (string $driver) {
    [$log, $publish] = attachTestLog($driver);
    $h = ServerHarness::start(log: $log);
    $cursor = $publish('old', 'before-reconnect');
    $initial = SseReader::open($h->url('/sse?topics=old'));
    $initial->next();
    $initial->next();
    expect($initial->next()['id'])->toBe($cursor);
    $initial->close();
    $retained = $publish('old', 'retained');
    $tail = $publish('new', 'before-join');

    $joining = SseReader::open($h->url('/sse?topics=old,new&fresh=new'), ['Last-Event-ID' => $cursor]);
    $joining->next();
    $joining->next();
    expect($joining->next()['id'])->toBe($retained);
    $marker = $joining->next();
    expect($marker['event'])->toBe('murmur.attached')->and($marker['id'])->toBe($tail);
    $joining->close();

    $missedNew = $publish('new', 'missed-new');
    $missedOld = $publish('old', 'missed-old');
    $resumed = SseReader::open($h->url('/sse?topics=old,new'), ['Last-Event-ID' => $marker['id']]);
    $resumed->next();
    $resumed->next();
    expect($resumed->next()['id'])->toBe($missedNew);
    expect($resumed->next()['id'])->toBe($missedOld);
    expect($resumed->next()['event'])->toBe('murmur.attached');
    $live = $publish('new', 'live');
    expect($resumed->next()['id'])->toBe($live);
})->with(['memory', 'redis']);

it('acknowledges a cursor below a stale client cursor and continues live', function (string $driver) {
    [$log, $publish] = attachTestLog($driver);
    $h = ServerHarness::start(log: $log);
    $tail = $publish('news', 'tail');
    $r = SseReader::open($h->url('/sse?topics=news&lastEventId=999999999999999999-0'));
    $r->next();
    $r->next();
    expect($r->nextRaw())->toBe("id: {$tail}\nevent: murmur.attached\ndata: {}");

    $live = $publish('news', 'live');
    expect($r->next()['id'])->toBe($live);
})->with(['memory', 'redis']);
