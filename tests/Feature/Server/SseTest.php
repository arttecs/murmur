<?php

use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SlowReplayLog;
use Arttecs\Murmur\Tests\Support\SseReader;
use React\Http\Browser;

use function React\Async\await;
use function React\Async\delay;

it('streams a published event to a public subscriber', function () {
    $h = ServerHarness::start();
    $r = SseReader::open($h->url('/sse?topics=news'));
    expect($r->status())->toBe(200);
    expect($r->next()['retry'])->toBe('3000');
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
    $r->next();
    expect($r->next()['event'])->toBe('b');
});

it('accepts the cursor in the query string', function () {
    $h = ServerHarness::start();
    $first = $h->log->append(new Event('news', 'a'));
    $h->log->append(new Event('news', 'b'));
    $r = SseReader::open($h->url("/sse?topics=news&lastEventId={$first->id}"));
    $r->next();
    expect($r->next()['event'])->toBe('b');
});

it('signals a gap when the cursor predates the log', function () {
    $h = ServerHarness::start();
    $r = SseReader::open($h->url('/sse?topics=news'), ['Last-Event-ID' => '1-0']);
    $r->next();
    expect($r->next()['event'])->toBe('murmur.gap');
});

it('treats a malformed cursor as no cursor', function () {
    $h = ServerHarness::start();
    $h->log->append(new Event('news', 'old'));
    $r = SseReader::open($h->url('/sse?topics=news'), ['Last-Event-ID' => 'nonsense']);
    expect($r->status())->toBe(200);
    $r->next();
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
    $r->next();
    expect([$r->next()['event'], $r->next()['event']])->toBe(['b', 'c']);
    expect(fn () => $r->next(0.2))->toThrow(Exception::class);
});

it('sends heartbeats', function () {
    $h = ServerHarness::start(['heartbeatSeconds' => 0.05]);
    $r = SseReader::open($h->url('/sse?topics=news'));
    $r->next();
    expect($r->next()['comment'])->toBe('ping');
});

it('closes the connection when its token expires', function () {
    $h = ServerHarness::start();
    $token = $h->token(['private-a'], ttl: 1);
    $r = SseReader::open($h->url("/sse?topics=private-a&token={$token}"));
    $r->next();
    expect(fn () => $r->next(2.0))->toThrow(Exception::class, 'closed');
});

it('releases the connection when the client disconnects', function () {
    $h = ServerHarness::start();
    $r = SseReader::open($h->url('/sse?topics=news'));
    $r->next();
    $r->close();
    delay(0.05);
    $body = (string) await((new Browser)->get($h->url('/health')))->getBody();
    expect(json_decode($body, true))->toBe(['status' => 'ok', 'connections' => 0]);
});

it('refuses connections above the limit', function () {
    $h = ServerHarness::start(['maxConnections' => 1]);
    SseReader::open($h->url('/sse?topics=news'))->next();
    expect(SseReader::open($h->url('/sse?topics=news'))->status())->toBe(503);
});

it('echoes an allowed origin', function () {
    $h = ServerHarness::start(['allowedOrigins' => ['https://app.test']]);
    $r = SseReader::open($h->url('/sse?topics=news'), ['Origin' => 'https://app.test']);
    expect($r->header('Access-Control-Allow-Origin'))->toBe('https://app.test');
});
