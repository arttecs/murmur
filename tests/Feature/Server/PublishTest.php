<?php

use Arttecs\Murmur\Server\Auth\Signature;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SseReader;
use React\EventLoop\Loop;
use React\Http\Browser;
use React\Stream\ThroughStream;

use function React\Async\await;

it('returns the assigned event ID as compact JSON', function () {
    $h = ServerHarness::start();
    $response = $h->publish('news', 'updated');

    expect($response->getStatusCode())->toBe(202);
    expect($response->getHeaderLine('Content-Type'))->toBe('application/json');
    expect((string) $response->getBody())->toMatch('/\A\{"id":"[0-9]+-[0-9]+"\}\z/');
});

it('rejects bad publishes', function () {
    $h = ServerHarness::start(['maxPayloadBytes' => 64]);
    expect($h->rawPublish('{"topic":"news","event":"x"}', signature: 'bad')->getStatusCode())->toBe(401);
    expect($h->rawPublish('{"topic":"a b","event":"x"}')->getStatusCode())->toBe(422);
    expect($h->rawPublish('{"topic":"news","event":"x\ny"}')->getStatusCode())->toBe(422);
    expect($h->rawPublish('not json')->getStatusCode())->toBe(422);
    expect($h->rawPublish(json_encode(['topic' => 'news', 'event' => 'x', 'data' => str_repeat('x', 100)]))->getStatusCode())->toBe(413);
});

it('rejects an oversized chunked publish before the body ends', function () {
    $secret = 'test-secret-with-at-least-32-bytes';
    $h = ServerHarness::start(['secret' => $secret, 'maxPayloadBytes' => 64]);
    $timestamp = time();
    $first = '{"topic":"news","event":"x","data":"';
    $second = str_repeat('x', 40).'"}';
    $body = new ThroughStream;
    $response = (new Browser)->withTimeout(1.0)->withRejectErrorResponse(false)->post($h->url('/publish'), [
        'Content-Type' => 'application/json',
        'X-Murmur-Timestamp' => (string) $timestamp,
        'X-Murmur-Signature' => Signature::sign($secret, $timestamp, $first.$second),
    ], $body);
    $timer = Loop::addTimer(1.0, fn () => $body->end());

    try {
        $body->write($first);
        $body->write($second);

        expect(await($response)->getStatusCode())->toBe(413);
        expect($body->isWritable())->toBeTrue();
    } finally {
        Loop::cancelTimer($timer);
        $body->close();
    }
});

it('rejects reserved event names other than the stream end marker', function (string $event, int $status) {
    $h = ServerHarness::start();
    $response = $h->publish('news', $event);

    expect($response->getStatusCode())->toBe($status);
})->with([
    ['murmur.gap', 422],
    ['murmur.attached', 422],
    ['murmur.heartbeat', 422],
    ['murmur.anything', 422],
    ['murmur.end', 202],
    ['murmurs.fine', 202],
]);

it('rejects a present but malformed excluded connection identifier', function (mixed $except) {
    $h = ServerHarness::start();

    expect($h->rawPublish(json_encode(['topic' => 'news', 'event' => 'updated', 'except' => $except]))->getStatusCode())->toBe(422);
})->with([[null], [''], [1], [str_repeat('A', 32)], [str_repeat('a', 32)."\n"]]);

it('only accepts UTF-8 topics within the byte limit', function (string $topic, bool $valid) {
    $h = ServerHarness::start();
    $encoded = json_encode($topic) ?: '"'.$topic.'"';
    $response = $h->rawPublish('{"topic":'.$encoded.',"event":"updated"}');

    expect($response->getStatusCode())->toBe($valid ? 202 : 422);
})->with('wire topic boundaries');

it('publishes ordered events to several topics', function () {
    $h = ServerHarness::start();
    $events = [];
    $h->log->subscribe(function ($event) use (&$events) {
        $events[] = $event;
    });
    $except = str_repeat('a', 32);
    $news = SseReader::open($h->url('/sse?topics=news'));
    $updates = SseReader::open($h->url('/sse?topics=updates'));
    $sender = SseReader::open($h->url('/sse?topics=news,updates&connection='.$except));
    $news->start();
    $updates->start();
    $sender->start();

    $response = $h->rawPublish(json_encode(['topics' => ['news', 'updates'], 'event' => 'x', 'except' => $except]));

    expect($response->getStatusCode())->toBe(202);
    expect(array_column($events, 'topic'))->toBe(['news', 'updates']);
    expect(array_column($events, 'except'))->toBe([$except, $except]);
    $ids = array_map(fn ($event) => (string) $event->id, $events);
    expect(json_decode((string) $response->getBody(), true))->toBe(['ids' => $ids]);
    expect(EventId::tryParse($ids[0])->compare(EventId::tryParse($ids[1])))->toBeLessThan(0);
    expect($news->next()['id'])->toBe($ids[0]);
    expect($updates->next()['id'])->toBe($ids[1]);

    $sentinel = $h->publish('news', 'sentinel');
    expect($sender->next()['id'])->toBe(json_decode((string) $sentinel->getBody(), true)['id']);
});

it('rejects invalid publish topic lists before appending', function (array $fields) {
    $h = ServerHarness::start(['maxTopicsPerPublish' => 2]);
    $events = [];
    $h->log->subscribe(function ($event) use (&$events) {
        $events[] = $event;
    });
    $response = $h->rawPublish(json_encode(['event' => 'x', ...$fields]));

    expect($response->getStatusCode())->toBe(422);
    expect($response->getHeaderLine('X-Murmur-Publish-Topics'))->toBe('2');
    expect($events)->toBe([]);
})->with([
    [['topic' => null, 'topics' => ['news']]],
    [[]],
    [['topics' => []]],
    [['topics' => ['news', 1]]],
    [['topics' => ['news', 'bad topic']]],
    [['topics' => ['news', 'news']]],
    [['topics' => ['news', 'updates', 'other']]],
    [['topics' => (object) ['0' => 'news']]],
]);
