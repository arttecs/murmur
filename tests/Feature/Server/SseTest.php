<?php

use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventIdGenerator;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SlowReplayLog;
use Arttecs\Murmur\Tests\Support\SseReader;

use function React\Async\delay;

it('streams a published event to a public subscriber', function () {
    $h = ServerHarness::start();
    $r = SseReader::open($h->url('/sse?topics=news'));
    expect($r->status())->toBe(200);
    expect($r->start()['retry'])->toBe('3000');
    expect($h->publish('news', 'updated', ['id' => 7])->getStatusCode())->toBe(202);
    $frame = $r->next();
    expect($frame['event'])->toBe('updated');
    expect(json_decode($frame['data'], true))->toBe(['topic' => 'news', 'data' => ['id' => 7]]);
});

it('requires a token for private topics', function () {
    $h = ServerHarness::start();
    expect(SseReader::open($h->url('/sse?topics=private-a'))->status())->toBe(401);
    expect(SseReader::open($h->url('/sse?topics=private-a&token=garbage'))->status())->toBe(401);
    $token = $h->token(['private-b']);
    expect(SseReader::open($h->url("/sse?topics=private-a&token={$token}"))->status())->toBe(403);
    $token = $h->token(['private-a']);
    expect(SseReader::open($h->url("/sse?topics=private-a,news&token={$token}"))->status())->toBe(200);
});

it('rejects malformed subscriptions', function (string $query) {
    $h = ServerHarness::start(['maxTopicsPerConnection' => 2]);
    expect(SseReader::open($h->url("/sse{$query}"))->status())->toBe(400);
})->with(['', '?topics=', '?topics=a%20b', '?topics=a,b,c']);

it('replays missed events after reconnecting', function () {
    $h = ServerHarness::start();
    $first = $h->log->append(new Event('news', 'a'));
    $h->log->append(new Event('other', 'skip'));
    $h->log->append(new Event('news', 'b'));
    $r = SseReader::open($h->url('/sse?topics=news'), ['Last-Event-ID' => (string) $first->id]);
    $r->start(replaying: true);
    expect($r->next()['event'])->toBe('b');
    $r->attached();
});

it('skips the event at the reconnect cursor when there is nothing to replay', function () {
    $log = new MemoryEventLog(ids: new EventIdGenerator(fn (): int => 1000));
    $h = ServerHarness::start(log: $log);
    $seen = $log->append(new Event('news', 'already-seen'));
    $r = SseReader::open($h->url('/sse?topics=news'), ['Last-Event-ID' => (string) $seen->id]);
    expect($r->start()['retry'])->toBe('3000');

    $log->append(new Event('news', 'fresh'));

    expect($r->next()['event'])->toBe('fresh');
});

it('accepts the cursor in the query string', function () {
    $h = ServerHarness::start();
    $first = $h->log->append(new Event('news', 'a'));
    $h->log->append(new Event('news', 'b'));
    $r = SseReader::open($h->url("/sse?topics=news&lastEventId={$first->id}"));
    $r->start(replaying: true);
    expect($r->next()['event'])->toBe('b');
    $r->attached();
});

it('falls back to the query cursor when the header is invalid', function (string $header) {
    $h = ServerHarness::start();
    $first = $h->log->append(new Event('news', 'a'));
    $h->log->append(new Event('news', 'b'));
    $r = SseReader::open($h->url("/sse?topics=news&lastEventId={$first->id}"), ['Last-Event-ID' => $header]);
    expect($r->start(replaying: true)['retry'])->toBe('3000');
    expect($r->next()['event'])->toBe('b');
    $r->attached();
})->with(['', 'nonsense']);

it('signals a gap when the cursor predates the log', function () {
    $h = ServerHarness::start();
    $r = SseReader::open($h->url('/sse?topics=news'), ['Last-Event-ID' => '1-0']);
    $r->start(replaying: true);
    expect($r->next()['event'])->toBe('murmur.gap');
    $r->attached();
});

it('treats a malformed cursor as no cursor', function () {
    $h = ServerHarness::start();
    $h->log->append(new Event('news', 'old'));
    $r = SseReader::open($h->url('/sse?topics=news'), ['Last-Event-ID' => 'nonsense']);
    expect($r->status())->toBe(200);
    $r->start();
    $h->publish('news', 'fresh');
    expect($r->next()['event'])->toBe('fresh');
});

it('delivers an event published during replay exactly once and in order', function () {
    $log = new SlowReplayLog(delaySeconds: 0.05);
    $h = ServerHarness::start(log: $log);
    $first = $log->append(new Event('news', 'a'));
    $log->append(new Event('news', 'b'));
    $r = SseReader::open($h->url('/sse?topics=news'), ['Last-Event-ID' => (string) $first->id]);
    $log->append(new Event('news', 'c'));
    $r->start(replaying: true);
    expect($r->next()['event'])->toBe('b');
    $r->attached();
    expect($r->next()['event'])->toBe('c');
    expect(fn () => $r->next(0.2))->toThrow(RuntimeException::class, 'Timed out');
});

it('sends heartbeats', function () {
    $h = ServerHarness::start(['heartbeatSeconds' => 0.05]);
    $r = SseReader::open($h->url('/sse?topics=news'));
    $r->start();
    expect($r->next()['comment'])->toBe('ping');
});

it('closes the connection when its token expires', function () {
    $h = ServerHarness::start();
    $token = $h->token(['private-a'], ttl: 2);
    $r = SseReader::open($h->url("/sse?topics=private-a&token={$token}"));
    expect($r->status())->toBe(200);
    $r->start();
    expect(fn () => $r->next(3.0))->toThrow(RuntimeException::class, 'closed');
});

it('releases the connection when the client disconnects', function () {
    $h = ServerHarness::start();
    $r = SseReader::open($h->url('/sse?topics=news'));
    $r->next();
    $r->close();

    $health = $h->pollHealth(0);
    expect($health->getStatusCode())->toBe(200);
    expect($health->getHeaderLine('Content-Type'))->toBe('application/json');
    expect((string) $health->getBody())->toBe('{"status":"ok","connections":0}');
});

it('releases the TCP socket when a non-reading subscriber is evicted', function () {
    $h = ServerHarness::start(['maxBufferBytes' => 65536]);
    $client = $h->subscribeWithoutReading('/sse?topics=news');

    try {
        expect((string) $h->pollHealth(1)->getBody())->toBe('{"status":"ok","connections":1}');
        expect($h->trackedSocketCount())->toBe(1);

        $h->log->append(new Event('news', 'large', str_repeat('x', 131072)));

        expect($h->trackedSocketCount())->toBe(0);
    } finally {
        $client->close();
    }
});

it('force-closes a stalled subscriber after the token expiry grace period', function () {
    $h = ServerHarness::start(['maxBufferBytes' => 32 * 1024 * 1024]);
    $token = $h->token(['private-a'], ttl: 2);
    $client = $h->subscribeWithoutReading("/sse?topics=private-a&token={$token}");

    try {
        expect((string) $h->pollHealth(1)->getBody())->toBe('{"status":"ok","connections":1}');
        $h->log->append(new Event('private-a', 'large', str_repeat('x', 16 * 1024 * 1024)));

        expect((string) $h->pollHealth(0, timeout: 2.5)->getBody())->toBe('{"status":"ok","connections":0}');
        expect($h->trackedSocketCount())->toBe(1);

        $deadline = hrtime(true) + 1_500_000_000;

        while ($h->trackedSocketCount() > 0 && hrtime(true) < $deadline) {
            delay(0.01);
        }

        expect($h->trackedSocketCount())->toBe(0);
    } finally {
        $client->close();
    }
});

it('refuses connections above the limit', function () {
    $h = ServerHarness::start(['maxConnections' => 1]);
    SseReader::open($h->url('/sse?topics=news'))->next();
    expect(SseReader::open($h->url('/sse?topics=news'))->status())->toBe(503);
});

it('echoes an allowed origin', function () {
    $h = ServerHarness::start(['allowedOrigins' => ['https://app.test']]);
    $r = SseReader::open($h->url('/sse?topics=news'), ['Origin' => 'HTTPS://APP.TEST']);
    expect($r->status())->toBe(200);
    expect($r->header('Access-Control-Allow-Origin'))->toBe('HTTPS://APP.TEST');
    expect($r->header('Vary'))->toBe('Origin');
});

it('signals a gap for a replay above the byte budget and continues live', function () {
    $h = ServerHarness::start();
    $first = $h->log->append(new Event('news', 'first'));

    for ($i = 0; $i < 900; $i++) {
        $tail = $h->log->append(new Event('news', 'history', str_repeat('x', 1200)));
    }

    $r = SseReader::open($h->url('/sse?topics=news'), ['Last-Event-ID' => (string) $first->id]);
    expect($r->start(replaying: true)['retry'])->toBe('3000');
    expect($r->next()['event'])->toBe('murmur.gap');
    expect($r->attached()['id'])->toBe((string) $tail->id);
    $h->publish('news', 'live');
    expect($r->next()['event'])->toBe('live');
});

it('continues serving sse after the request deadline', function () {
    $h = ServerHarness::start(['requestTimeoutSeconds' => 0.1]);
    $r = SseReader::open($h->url('/sse?topics=news'));
    expect($r->start()['retry'])->toBe('3000');

    delay(0.15);

    expect($h->publish('news', 'live')->getStatusCode())->toBe(202);
    expect($r->next()['event'])->toBe('live');
});

it('delivers live events with a cursor ahead of the memory log', function () {
    $h = ServerHarness::start();
    $h->log->append(new Event('news', 'old'));
    $r = SseReader::open($h->url('/sse?topics=news'), ['Last-Event-ID' => '999999999999999999-0']);
    expect($r->start()['retry'])->toBe('3000');

    $h->publish('news', 'live');

    expect($r->next()['event'])->toBe('live');
});

it('serves requests without an origin by default and sends no allow origin header', function () {
    $h = ServerHarness::start();
    $r = SseReader::open($h->url('/sse?topics=news'));

    expect($r->status())->toBe(200);
    expect($r->header('Access-Control-Allow-Origin'))->toBeNull();
    expect($r->header('Vary'))->toBe('Origin');
});

it('rejects any origin by default and counts the rejection', function () {
    $h = ServerHarness::start();
    $r = SseReader::open($h->url('/sse?topics=news'), ['Origin' => 'https://other.test']);

    expect($r->status())->toBe(403);
    expect($r->header('Access-Control-Allow-Origin'))->toBeNull();
    expect($r->header('Vary'))->toBe('Origin');
    expect($h->metrics()->value('murmur_connections_rejected_total', ['status' => '403']))->toBe(1);
});

it('treats the scheme, host and port as exact origin components', function (string $origin) {
    $h = ServerHarness::start(['allowedOrigins' => ['https://app.test:8443']]);
    $r = SseReader::open($h->url('/sse?topics=news'), ['Origin' => $origin]);

    expect($r->status())->toBe(403);
    expect($r->header('Access-Control-Allow-Origin'))->toBeNull();
})->with(['http://app.test:8443', 'https://other.test:8443', 'https://app.test', 'https://app.test:443', 'null', '']);

it('allows any origin when configured with a wildcard', function () {
    $h = ServerHarness::start(['allowedOrigins' => ['*']]);
    $r = SseReader::open($h->url('/sse?topics=news'), ['Origin' => 'null']);

    expect($r->status())->toBe(200);
    expect($r->header('Access-Control-Allow-Origin'))->toBe('*');
    expect($r->header('Vary'))->toBeNull();
});

it('announces the connection after retry and before delivering events', function (bool $replaying) {
    $log = new SlowReplayLog(delaySeconds: 0.05);
    $h = ServerHarness::start(log: $log);
    $first = $log->append(new Event('news', 'cursor'));
    $query = $replaying ? '&lastEventId='.$first->id : '';

    $history = $log->append(new Event('news', 'history'));
    $r = SseReader::open($h->url('/sse?topics=news'.$query));
    $log->append(new Event('news', 'live'));

    expect($r->next()['retry'])->toBe('3000');
    $frame = $r->next();
    expect($frame['event'])->toBe('murmur.connected')->and($frame['id'])->toBeNull();
    $data = json_decode($frame['data'], true, flags: JSON_THROW_ON_ERROR);
    expect($data)->toHaveKeys(['connection_id', 'protocol']);
    expect($data['protocol'])->toBe(1);
    expect($data['connection_id'])->toMatch('/\A[0-9a-f]{32}\z/');

    if ($replaying) {
        expect($r->next()['event'])->toBe('history');
    }

    expect($r->attached()['id'])->toBe((string) $history->id);
    expect($r->next()['event'])->toBe('live');

    $reopened = SseReader::open($h->url('/sse?topics=news&connection='.$data['connection_id']));
    expect($reopened->next()['retry'])->toBe('3000');
    expect($reopened->next())->toBe($frame);
    $reopened->attached();
})->with([false, true]);

it('rejects malformed connection identifiers', function (string $query) {
    $h = ServerHarness::start();

    expect(SseReader::open($h->url('/sse?topics=news&'.$query))->status())->toBe(400);
})->with(['connection=', 'connection=short', 'connection='.str_repeat('A', 32), 'connection='.str_repeat('a', 32).'%0A', 'connection[]=id']);

it('excludes matching connections in live and replay delivery without charging them for the frames', function () {
    $h = ServerHarness::start(['maxBufferBytes' => 512]);
    $id = str_repeat('a', 32);
    $path = '/sse?topics=news&connection='.$id;
    $first = $h->log->append(new Event('news', 'cursor'));
    $sender = SseReader::open($h->url($path));
    $sameId = SseReader::open($h->url($path));
    $other = SseReader::open($h->url('/sse?topics=news'));

    foreach ([$sender, $sameId, $other] as $reader) {
        $reader->start();
    }

    $excluded = $h->rawPublish(json_encode(['topic' => 'news', 'event' => 'excluded', 'data' => str_repeat('x', 1024), 'except' => $id]));
    expect($excluded->getStatusCode())->toBe(202);
    expect($other->next()['event'])->toBe('excluded');
    expect($h->metrics()->value('murmur_events_sent_total'))->toBe(1);

    $reopened = SseReader::open($h->url($path.'&lastEventId='.$first->id));
    $reopened->start();
    $next = $h->log->append(new Event('news', 'next'));

    foreach ([$sender, $sameId, $other, $reopened] as $reader) {
        $frame = $reader->next();
        expect($frame['id'])->toBe((string) $next->id);
        expect($frame['data'])->toBe('{"topic":"news","data":null}');
    }

    expect($h->metrics()->value('murmur_events_sent_total'))->toBe(5);
    expect($h->metrics()->value('murmur_gaps_total'))->toBe(0);
});

it('counts only deliverable events against the replay limit', function () {
    $h = ServerHarness::start(['maxReplayEvents' => 1]);
    $id = str_repeat('a', 32);
    $cursor = $h->log->append(new Event('news', 'cursor'));
    $h->log->append(new Event('news', 'excluded', except: $id));
    $visible = $h->log->append(new Event('news', 'visible'));

    $r = SseReader::open($h->url("/sse?topics=news&connection={$id}&lastEventId={$cursor->id}"));
    $r->start(replaying: true);
    expect($r->next()['id'])->toBe((string) $visible->id);
    $r->attached();
});
