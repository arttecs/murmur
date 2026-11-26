<?php

use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Http\HeldConnection;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Server\Replay;
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
    $connection = new HeldConnection($this->stream, cursor: new EventId(10, 0));
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

    $connection->release(new Replay(false, [$replayed], $replayed->id));
    expect($this->frames)->toBe([$retry, $gap, Frame::event($replayed), Frame::attached($replayed->id), Frame::event($queued), $heartbeat]);

    $connection->sendEvent($live);
    expect($this->frames)->toBe([$retry, $gap, Frame::event($replayed), Frame::attached($replayed->id), Frame::event($queued), $heartbeat, Frame::event($live)]);
});

it('drops held events when the connection closes before release', function () {
    $connection = new HeldConnection($this->stream);
    $connection->sendEvent(new Event('news', 'held'));

    $connection->close();
    $connection->release(new Replay(false, [new Event('news', 'replay')], null));

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
    $connection = new HeldConnection($this->stream, cursor: new EventId(10, 0));
    $replayed = new Event('news', 'replay', id: new EventId(10, 2));
    $held = new Event('news', 'held', id: new EventId(10, 3));
    $live = new Event('news', 'live', id: new EventId(10, 4));
    $connection->sendEvent($held);
    $connection->sendEvent($replayed);
    $connection->release(new Replay(false, [$replayed], $replayed->id));

    $connection->sendEvent($replayed);
    $connection->sendEvent($held);
    $connection->sendEvent($live);
    $connection->sendEvent($live);
    $connection->sendEvent($held);

    expect($this->frames)->toBe([Frame::event($replayed), Frame::attached($replayed->id), Frame::event($held), Frame::event($live)]);
});

it('keeps the marker and watermark at the last written id when replay exceeds the sampled tail', function () {
    $connection = new HeldConnection($this->stream, cursor: new EventId(10, 1));
    $event = new Event('news', 'ahead-of-tail', id: new EventId(10, 3));

    $connection->release(new Replay(false, [$event], new EventId(10, 2)));
    $connection->sendEvent($event);

    expect($this->frames)->toBe([
        Frame::event($event),
        "id: 10-3\nevent: murmur.attached\ndata: {}\n\n",
    ]);
});

it('always writes events without an id without resetting its last written id', function () {
    $cursor = new EventId(10, 2);
    $connection = new HeldConnection($this->stream, cursor: $cursor);
    $alreadySeen = new Event('news', 'old', id: $cursor);
    $unidentified = new Event('news', 'unidentified');
    $connection->sendEvent($alreadySeen);
    $connection->sendEvent($unidentified);
    $connection->release(new Replay(false, [$alreadySeen, $unidentified], $cursor));

    $connection->sendEvent($unidentified);
    $connection->sendEvent($alreadySeen);

    expect($this->frames)->toBe([Frame::event($unidentified), Frame::attached($cursor), Frame::event($unidentified), Frame::event($unidentified)]);
});

it('delivers the next memory event even when the cursor is ahead of the log', function (bool $empty) {
    $log = new MemoryEventLog;
    $tail = $empty ? null : $log->append(new Event('news', 'old'))->id;
    $cursor = new EventId(999999999999999999, 0);
    $connection = new HeldConnection($this->stream, cursor: $cursor);
    $log->subscribe($connection->sendEvent(...));

    $replay = React\Async\await($log->since($cursor, ['news'], 1000));
    expect($replay->tail)->toBe($tail);
    $connection->release($replay);
    $live = $log->append(new Event('news', 'live'));

    expect($this->frames)->toBe([Frame::gap(), Frame::attached($tail), Frame::event($live)]);
})->with(['empty log' => [true], 'nonempty log' => [false]]);

it('reports a gap once when a gap frame arrives while a replay that has a gap is pending', function () {
    $stream = new ThroughStream;
    $output = '';
    $stream->on('data', function (string $data) use (&$output): void {
        $output .= $data;
    });
    $connection = new HeldConnection($stream, 1048576);
    $connection->send(Frame::gap());
    $connection->release(new Replay(true, [], null));

    expect(substr_count($output, 'event: murmur.gap'))->toBe(1);
});

it('keeps a gap frame that arrives while a replay without a gap is pending', function () {
    $stream = new ThroughStream;
    $output = '';
    $stream->on('data', function (string $data) use (&$output): void {
        $output .= $data;
    });
    $connection = new HeldConnection($stream, 1048576);
    $connection->send(Frame::gap());
    $connection->release(new Replay(false, [], null));

    expect(substr_count($output, 'event: murmur.gap'))->toBe(1);
});

it('steps over undispatched history and releases newer held events exactly once', function () {
    $cursor = new EventId(10, 1);
    $connection = new HeldConnection($this->stream, cursor: $cursor);
    $replayed = new Event('news', 'replayed', id: new EventId(10, 2));
    $old = new Event('news', 'lagging', id: new EventId(10, 3));
    $live = new Event('news', 'held', id: new EventId(10, 5));
    $connection->sendEvent($old);
    $connection->sendEvent($live);

    $connection->release(new Replay(true, [$replayed], new EventId(10, 4)));
    $connection->sendEvent($old);
    $connection->sendEvent($live);
    $connection->release();

    expect($this->frames)->toBe([
        "event: murmur.gap\ndata: {}\n\n",
        Frame::event($replayed),
        "id: 10-4\nevent: murmur.attached\ndata: {}\n\n",
        Frame::event($live),
    ]);
});
