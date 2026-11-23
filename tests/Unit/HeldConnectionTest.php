<?php

use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Http\HeldConnection;
use Arttecs\Murmur\Server\Sse\Frame;
use React\Stream\ThroughStream;

beforeEach(function () {
    $this->stream = new ThroughStream;
    $this->frames = [];
    $this->stream->on('data', function (string $frame): void {
        $this->frames[] = $frame;
    });
});

it('writes replay before held events and sends subsequent events immediately', function () {
    $connection = new HeldConnection($this->stream);
    $replayed = new Event('news', 'replay', id: new EventId(10, 2));
    $queued = new Event('news', 'queued', id: new EventId(10, 3));
    $live = new Event('news', 'live', id: new EventId(10, 4));
    $heartbeat = Frame::comment();

    $connection->sendEvent(new Event('news', 'older', id: new EventId(10, 1)));
    $connection->sendEvent(new Event('news', 'duplicate', id: new EventId(10, 2)));
    $connection->sendEvent($queued);
    $connection->send($heartbeat);
    expect($this->frames)->toBe([]);

    $retry = Frame::retry(3000);
    $gap = Frame::gap();
    $connection->sendImmediately($retry);
    $connection->sendImmediately($gap);
    expect($this->frames)->toBe([$retry, $gap]);

    $connection->release([$replayed]);
    expect($this->frames)->toBe([$retry, $gap, Frame::event($replayed), Frame::event($queued), $heartbeat]);

    $connection->sendEvent($live);
    expect($this->frames)->toBe([$retry, $gap, Frame::event($replayed), Frame::event($queued), $heartbeat, Frame::event($live)]);
});

it('drops held events when the connection closes before release', function () {
    $connection = new HeldConnection($this->stream);
    $connection->sendEvent(new Event('news', 'held'));

    $connection->close();
    $connection->release([new Event('news', 'replay')]);

    expect($connection->isClosed())->toBeTrue();
    expect($this->frames)->toBe([]);
});

it('evicts a held connection when its encoded event bytes exceed the limit', function () {
    $first = new Event('news', 'first', ['message' => 'hello'], new EventId(10, 1));
    $second = new Event('news', 'second', id: new EventId(10, 2));
    $connection = new HeldConnection($this->stream, maxBufferBytes: strlen(Frame::event($first).Frame::event($second)));

    $connection->sendEvent($first);
    $connection->sendEvent($second);
    expect($connection->isClosed())->toBeFalse();

    $connection->sendEvent(new Event('news', 'overflow'));
    expect($connection->isClosed())->toBeTrue();
    expect($this->stream->isWritable())->toBeFalse();
});

it('drops events at or behind the last written id after going live', function () {
    $connection = new HeldConnection($this->stream);
    $replayed = new Event('news', 'replay', id: new EventId(10, 2));
    $held = new Event('news', 'held', id: new EventId(10, 3));
    $live = new Event('news', 'live', id: new EventId(10, 4));
    $connection->sendEvent($held);
    $connection->sendEvent($replayed);
    $connection->release([$replayed]);

    $connection->sendEvent($replayed);
    $connection->sendEvent($held);
    $connection->sendEvent($live);
    $connection->sendEvent($live);
    $connection->sendEvent($held);

    expect($this->frames)->toBe([Frame::event($replayed), Frame::event($held), Frame::event($live)]);
});

it('always writes events without an id without resetting its last written id', function () {
    $cursor = new EventId(10, 2);
    $connection = new HeldConnection($this->stream, lastWrittenId: $cursor);
    $alreadySeen = new Event('news', 'old', id: $cursor);
    $unidentified = new Event('news', 'unidentified');
    $connection->sendEvent($alreadySeen);
    $connection->sendEvent($unidentified);
    $connection->release([$alreadySeen, $unidentified]);

    $connection->sendEvent($unidentified);
    $connection->sendEvent($alreadySeen);

    expect($this->frames)->toBe(array_fill(0, 3, Frame::event($unidentified)));
});
