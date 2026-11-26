<?php

use Arttecs\Murmur\Server\Auth\TokenIssuer;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SlowReplayLog;
use Arttecs\Murmur\Tests\Support\SseReader;

it('uses the union of verified tokens to authorize private topics', function () {
    $h = ServerHarness::start();
    $a = $h->token(['private-a']);
    $b = $h->token(['private-b']);

    expect(SseReader::open($h->url("/sse?topics=private-a,private-b&token={$a},{$b}"))->status())->toBe(200);
    expect(SseReader::open($h->url("/sse?topics=private-a,private-b,private-c&token={$a},{$b}"))->status())->toBe(403);
});

it('rejects an invalid token even when another token covers the requested topic', function (string $kind) {
    $h = ServerHarness::start();
    $valid = $h->token(['private-a']);
    $bad = match ($kind) {
        'expired' => $h->token(['private-a'], ttl: -1),
        'signature' => (new TokenIssuer(str_repeat('x', 32)))->issue(['private-a'], 60),
        default => 'garbage',
    };

    expect(SseReader::open($h->url("/sse?topics=private-a&token={$valid},{$bad}"))->status())->toBe(401);
    expect(SseReader::open($h->url("/sse?topics=news&token={$bad}"))->status())->toBe(401);
})->with(['expired', 'signature', 'garbage']);

it('rejects token lists that are empty, malformed or over the topic limit', function (string $query) {
    $h = ServerHarness::start(['maxTopicsPerConnection' => 2]);
    $valid = $h->token(['private-a']);
    $query = str_replace('TOKEN', $valid, $query);

    expect(SseReader::open($h->url('/sse?topics=private-a&'.$query))->status())->toBe(400);
})->with(['token=', 'token[]=TOKEN', 'token=TOKEN,,TOKEN', 'token=,TOKEN', 'token=TOKEN,', 'token=TOKEN,TOKEN,TOKEN']);

it('closes at the first expiry that would leave a requested private topic uncovered', function () {
    $h = ServerHarness::start();
    $redundant = $h->token(['private-a', 'private-b'], ttl: 1);
    $early = $h->token(['private-a'], ttl: 3);
    $late = $h->token(['private-b'], ttl: 15);
    $r = SseReader::open($h->url("/sse?topics=private-a,private-b&token={$redundant},{$early},{$late}"));

    expect($r->status())->toBe(200);
    $r->start();
    React\Async\delay(1.05);
    expect($h->metrics()->value('murmur_connections'))->toBe(1);
    expect(fn () => $r->next(3.0))->toThrow(RuntimeException::class, 'closed');
});

it('ignores expiry of unused tokens on a public-only subscription', function () {
    $h = ServerHarness::start(['heartbeatSeconds' => 0.05]);
    $token = $h->token(['private-a'], ttl: 1);
    $r = SseReader::open($h->url("/sse?topics=news&token={$token}"));
    $r->start();

    React\Async\delay(1.05);
    expect(json_decode((string) $h->pollHealth(1)->getBody(), true)['connections'])->toBe(1);
});

it('rejects invalid fresh topics and topics outside the subscription', function (string $fresh) {
    $h = ServerHarness::start();

    expect(SseReader::open($h->url('/sse?topics=old,new&'.$fresh))->status())->toBe(400);
})->with(['fresh=', 'fresh[]=new', 'fresh=other', 'fresh=old,,new', 'fresh=old,new%20']);

it('only delivers post-attach events for fresh topics and preserves order across topics', function (bool $cursor) {
    $log = new SlowReplayLog(delaySeconds: 0.05);
    $h = ServerHarness::start(log: $log);
    $first = $log->append(new Event('old', 'cursor'));
    $old = $log->append(new Event('old', 'retained'));
    $tail = $log->append(new Event('new', 'before-joining'));

    $r = SseReader::open($h->url('/sse?topics=old,new&fresh=new'.($cursor ? '&lastEventId='.$first->id : '')));
    $r->start(replaying: true);

    if ($cursor) {
        expect($r->next()['id'])->toBe((string) $old->id);
    }

    expect($r->attached()['id'])->toBe((string) $tail->id);
    $fresh = $log->append(new Event('new', 'live-fresh'));
    $late = $log->append(new Event('old', 'live-old'));

    foreach ([$fresh, $late] as $event) {
        expect($r->next()['id'])->toBe((string) $event->id);
    }

    expect(fn () => $r->next(0.05))->toThrow(RuntimeException::class, 'Timed out');
})->with([false, true]);

it('preserves the trim gap when the missing history may belong to fresh topics', function () {
    $h = ServerHarness::start(log: new MemoryEventLog(capacity: 2));
    $cursor = $h->log->append(new Event('old', 'cursor'));
    $h->log->append(new Event('new', 'trimmed'));
    $h->log->append(new Event('new', 'retained-fresh'));
    $retained = $h->log->append(new Event('old', 'retained-old'));

    $r = SseReader::open($h->url("/sse?topics=old,new&fresh=new&lastEventId={$cursor->id}"));
    $r->start(replaying: true);
    expect($r->next()['event'])->toBe('murmur.gap');
    expect($r->next()['id'])->toBe((string) $retained->id);
    expect($r->attached()['id'])->toBe((string) $retained->id);
});
