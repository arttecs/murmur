<?php

use Arttecs\Murmur\Server\Auth\Grant;
use Arttecs\Murmur\Server\Auth\TokenIssuer;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Tests\Support\CallbackSubscriptionAuthorizer;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SlowReplayLog;
use Arttecs\Murmur\Tests\Support\SseReader;
use Firebase\JWT\JWT;
use React\Promise\PromiseInterface;

use function React\Async\delay;
use function React\Promise\resolve;

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
    $redundantExpiry = null;
    $authorizer = new CallbackSubscriptionAuthorizer(function (string $token) use (&$redundantExpiry): PromiseInterface {
        $redundantExpiry ??= time() + 2;

        return resolve(match ($token) {
            'redundant' => new Grant(['private-a', 'private-b'], $redundantExpiry),
            'early' => new Grant(['private-a'], $redundantExpiry + 2),
            'late' => new Grant(['private-b'], $redundantExpiry + 15),
        });
    });
    $h = ServerHarness::start(authorizer: $authorizer);
    $r = SseReader::open($h->url('/sse?topics=private-a,private-b&token=redundant,early,late'));

    expect($r->status())->toBe(200);
    $r->start();
    delay(max(0.0, $redundantExpiry - microtime(true)) + 0.05);
    expect($h->metrics()->value('murmur_connections'))->toBe(1);
    expect(fn () => $r->next(3.0))->toThrow(RuntimeException::class, 'closed');
});

it('ignores expiry of unused tokens on a public-only subscription', function () {
    $expiry = null;
    $authorizer = new CallbackSubscriptionAuthorizer(function () use (&$expiry): PromiseInterface {
        return resolve(new Grant(['private-a'], $expiry = time() + 2));
    });
    $h = ServerHarness::start(authorizer: $authorizer);
    $r = SseReader::open($h->url('/sse?topics=news&token=unused'));
    $r->start();

    delay(max(0.0, $expiry - microtime(true)) + 0.05);
    expect($h->metrics()->value('murmur_connections'))->toBe(1);
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

it('rejects an oversized request head even within the token count limit', function () {
    $h = ServerHarness::start();
    $topics = array_map(fn (int $i): string => 'private-'.str_repeat('a', 150).$i, range(1, 30));
    $tokens = array_map(fn (string $topic): string => $h->token([$topic]), $topics);
    $path = '/sse?'.http_build_query(['topics' => implode(',', $topics), 'token' => implode(',', $tokens)]);

    expect(SseReader::open($h->url($path))->status())->toBe(431);
    expect($h->metrics()->value('murmur_connections_opened_total'))->toBe(0);
});

it('treats a signed token with invalid grant topics as an invalid credential', function () {
    $secret = str_repeat('x', 32);
    $h = ServerHarness::start(['secret' => $secret]);
    $token = JWT::encode(['topics' => ["private-a\n"], 'exp' => time() + 60], $secret, 'HS256');

    expect(SseReader::open($h->url("/sse?topics=news&token={$token}"))->status())->toBe(401);
});

it('withholds excluded events that arrive while the connection is held', function () {
    $log = new SlowReplayLog(delaySeconds: 0.05);
    $h = ServerHarness::start(log: $log);
    $id = str_repeat('a', 32);
    $cursor = $log->append(new Event('news', 'cursor'))->id;
    $r = SseReader::open($h->url("/sse?topics=news&connection={$id}&lastEventId={$cursor}"));
    $r->start(replaying: true);

    $log->append(new Event('news', 'excluded', except: $id));
    $visible = $log->append(new Event('news', 'visible'))->id;

    expect($r->attached()['id'])->toBe((string) $cursor);
    expect($r->next()['id'])->toBe((string) $visible);
    expect($h->metrics()->value('murmur_events_sent_total'))->toBe(1);
});

it('applies the topic byte limit and UTF-8 validation to subscriptions and fresh topics', function (string $topic, bool $valid) {
    $h = ServerHarness::start();
    $topic = rawurlencode($topic);

    expect(SseReader::open($h->url("/sse?topics={$topic}"))->status())->toBe($valid ? 200 : 400);
    expect(SseReader::open($h->url("/sse?topics={$topic}&fresh={$topic}"))->status())->toBe($valid ? 200 : 400);
})->with('wire topic boundaries');
