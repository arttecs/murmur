<?php

use Arttecs\Murmur\Server\Connection;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Http\HeldConnection;
use Arttecs\Murmur\Server\Hub;
use Arttecs\Murmur\Server\Replay;
use Arttecs\Murmur\Server\Sse\Frame;

it('delivers an event only to connections subscribed to its topic', function () {
    $hub = new Hub;
    [$a, $outA] = fakeConnection();
    [$b, $outB] = fakeConnection();
    $hub->attach($a, ['news']);
    $hub->attach($b, ['other']);
    $hub->dispatch(new Event('news', 'x', null, new EventId(1, 0)));
    expect($outA())->toContain('event: x');
    expect($outB())->toBe('');
});

it('writes a single frame to a connection subscribed to several topics', function () {
    $hub = new Hub;
    [$a, $out] = fakeConnection();
    $hub->attach($a, ['news', 'other']);
    $hub->dispatch(new Event('news', 'x', null, new EventId(1, 0)));
    expect(substr_count($out(), 'event: x'))->toBe(1);
});

it('dispatches events through the connection so it can drop ids it already sent', function () {
    $hub = new Hub;
    [, $output, $stream] = fakeConnection();
    $connection = new HeldConnection($stream);
    $hub->attach($connection, ['news']);
    $connection->release();
    $event = new Event('news', 'updated', id: new EventId(10, 1));

    $hub->dispatch($event);
    $hub->dispatch($event);

    expect($output())->toBe(Frame::attached(null).Frame::event($event));
});

it('forgets a connection when its stream closes', function () {
    $hub = new Hub;
    [$a, , $stream] = fakeConnection();
    $hub->attach($a, ['news']);
    $stream->close();
    expect($hub->connectionCount())->toBe(0);
});

it('sends heartbeats to every connection', function () {
    $hub = new Hub;
    [$a, $out] = fakeConnection();
    $hub->attach($a, ['news']);
    $hub->heartbeat();
    expect($out())->toBe(": ping\n\n");
});

it('advances an idle connection past dispatched events and still delivers newer events exactly once', function () {
    $hub = new Hub;
    [, $out, $stream] = fakeConnection();
    $connection = new HeldConnection($stream);
    $hub->attach($connection, ['news']);
    $connection->release();

    $hub->dispatch(new Event('other', 'unrelated', id: new EventId(10, 2)));
    $hub->heartbeat();
    $hub->dispatch(new Event('news', 'already-passed', id: new EventId(10, 2)));
    $live = new Event('news', 'live', id: new EventId(10, 3));
    $hub->dispatch($live);
    $hub->dispatch($live);

    expect($out())->toBe(Frame::attached(null)."id: 10-2\nevent: murmur.heartbeat\ndata: {}\n\n".Frame::event($live));
});

it('uses a comment heartbeat while held and while the dispatched position is behind the marker', function () {
    $hub = new Hub;
    [, $out, $stream] = fakeConnection();
    $connection = new HeldConnection($stream);
    $hub->attach($connection, ['news']);

    $hub->dispatch(new Event('news', 'held-fresh', id: new EventId(10, 2)));
    $hub->heartbeat();
    expect($out())->toBe('');

    $tail = new EventId(10, 3);
    $connection->release(new Replay(false, [], $tail), ['news']);
    $hub->heartbeat();
    $hub->dispatch(new Event('news', 'late-fresh', id: $tail));

    expect($out())->toBe(Frame::attached($tail).": ping\n\n: ping\n\n");

    $hub->heartbeat();
    expect($out())->toBe(Frame::attached($tail).": ping\n\n: ping\n\nid: 10-3\nevent: murmur.heartbeat\ndata: {}\n\n");
});

it('advances a heartbeat past an excluded event without delivering it', function () {
    $hub = new Hub;
    [, $out, $stream] = fakeConnection();
    $connection = new HeldConnection($stream);
    $hub->attach($connection, ['news']);
    $connection->release();

    $hub->dispatch(new Event('news', 'excluded', id: new EventId(10, 2), except: $connection->id));
    $hub->heartbeat();

    expect($out())->toBe(Frame::attached(null)."id: 10-2\nevent: murmur.heartbeat\ndata: {}\n\n");
});

it('sends one gap frame per connection and continues after a subscriber write fails', function () {
    $errorLog = tempnam(sys_get_temp_dir(), 'murmur');
    $previous = ini_set('error_log', $errorLog);

    try {
        $hub = new Hub;
        [$failing, , $stream] = fakeConnection();
        $stream->on('data', fn () => throw new RuntimeException('The gap write failed.'));
        [$a, $outA] = fakeConnection();
        [$b, $outB] = fakeConnection();
        $hub->attach($failing, ['news']);
        $hub->attach($a, ['news', 'other']);
        $hub->attach($b, ['other']);

        $hub->gap();

        expect($failing->isClosed())->toBeTrue();
        expect($outA())->toBe("event: murmur.gap\ndata: {}\n\n");
        expect($outB())->toBe($outA());
    } finally {
        ini_set('error_log', (string) $previous);
        unlink($errorLog);
    }
});

it('stops delivering to a detached connection', function () {
    $hub = new Hub;
    [$a, $out] = fakeConnection();
    $hub->attach($a, ['news']);
    $hub->detach($a);
    $hub->dispatch(new Event('news', 'x', null, new EventId(1, 0)));

    expect($out())->toBe('');
    expect($hub->connectionCount())->toBe(0);
    expect($a->isClosed())->toBeFalse();
});

it('replaces the topics of a connection that attaches again', function () {
    $hub = new Hub;
    [$a, $out] = fakeConnection();
    $hub->attach($a, ['news']);
    $hub->attach($a, ['other']);
    $hub->dispatch(new Event('news', 'x', null, new EventId(1, 0)));
    $hub->dispatch(new Event('other', 'y', null, new EventId(2, 0)));

    expect($out())->not->toContain('event: x');
    expect($out())->toContain('event: y');
    expect($hub->connectionCount())->toBe(1);
});

it('closes every connection with the shutdown reason', function () {
    $hub = new Hub;
    [$a] = fakeConnection();
    [$b] = fakeConnection();
    $hub->attach($a, ['news']);
    $hub->attach($b, ['other']);
    $reasons = [];
    $capture = function (string $reason) use (&$reasons): void {
        $reasons[] = $reason;
    };
    $a->onClose($capture);
    $b->onClose($capture);

    $hub->closeAll();

    expect($reasons)->toBe(['shutdown', 'shutdown']);
    expect($a->isClosed())->toBeTrue();
    expect($b->isClosed())->toBeTrue();
    expect($hub->connectionCount())->toBe(0);
});

it('closes a failed connection, reports its error and continues fan-out', function () {
    \React\Async\delay(1.01);
    $errorLog = tempnam(sys_get_temp_dir(), 'murmur');
    $previous = ini_set('error_log', $errorLog);

    try {
        $hub = new Hub;
        [$failing, , $stream] = fakeConnection();
        $stream->on('data', fn () => throw new RuntimeException('The subscriber write failed.'));
        [$healthy, $output] = fakeConnection();
        $hub->attach($failing, ['news']);
        $hub->attach($healthy, ['news']);
        $reasons = [];
        $failing->onClose(function (string $reason) use (&$reasons): void {
            $reasons[] = $reason;
        });

        $hub->dispatch(new Event('news', 'updated'));

        expect($failing->isClosed())->toBeTrue();
        expect($hub->connectionCount())->toBe(1);
        expect($reasons)->toBe(['error']);
        expect($output())->toContain("event: updated\n");
        expect(file_get_contents($errorLog))->toContain('The subscriber write failed.');
    } finally {
        ini_set('error_log', (string) $previous);
        unlink($errorLog);
    }
});

it('encodes a dispatched event once across held and live connections', function () {
    $payload = new class implements JsonSerializable
    {
        public int $encodes = 0;

        public function jsonSerialize(): string
        {
            $this->encodes++;

            return 'data';
        }
    };

    $hub = new Hub;
    [$live, $liveOutput] = fakeConnection();
    [, $heldOutput, $stream] = fakeConnection();
    $held = new HeldConnection($stream);
    $hub->attach($live, ['news']);
    $hub->attach($held, ['news']);

    $hub->dispatch(new Event('news', 'updated', $payload));
    $held->release();

    expect($liveOutput())->toContain('event: updated');
    expect($heldOutput())->toBe(Frame::attached(null).$liveOutput());
    expect($payload->encodes)->toBe(1);
});

it('sends one heartbeat per connection over a full rotation', function () {
    $hub = new Hub(heartbeatSlices: 4);
    $outputs = [];

    for ($i = 0; $i < 8; $i++) {
        [$connection, $outputs[]] = fakeConnection();
        $hub->attach($connection, ['news']);
    }

    for ($tick = 0; $tick < 4; $tick++) {
        $hub->heartbeatSlice();
    }

    expect(array_map(fn ($out) => $out(), $outputs))->toBe(array_fill(0, 8, Frame::comment()));

    for ($tick = 0; $tick < 4; $tick++) {
        $hub->heartbeatSlice();
    }

    expect(array_map(fn ($out) => $out(), $outputs))->toBe(array_fill(0, 8, Frame::comment().Frame::comment()));
});

it('delays the first heartbeat of a new connection by at least half a rotation', function () {
    $hub = new Hub(heartbeatSlices: 4);
    $hub->heartbeatSlice();
    $outputs = [];

    for ($i = 0; $i < 6; $i++) {
        [$connection, $outputs[]] = fakeConnection();
        $hub->attach($connection, ['news']);
    }

    $written = fn (): int => count(array_filter($outputs, fn ($out) => $out() !== ''));

    $hub->heartbeatSlice();
    $hub->heartbeatSlice();
    expect($written())->toBe(0);

    $hub->heartbeatSlice();
    expect($written())->toBe(3);

    $hub->heartbeatSlice();
    expect($written())->toBe(6);
});

it('spreads a burst of connections evenly across the eligible heartbeat slices', function () {
    $hub = new class(heartbeatSlices: 6) extends Hub
    {
        /**
         * Get the number of connections in each heartbeat bucket.
         */
        public function bucketSizes(): array
        {
            return array_map('count', $this->heartbeatBuckets);
        }
    };

    for ($i = 0; $i < 10; $i++) {
        [$connection] = fakeConnection();
        $hub->attach($connection, ['news']);
    }

    expect($hub->bucketSizes())->toBe([0, 0, 0, 3, 3, 4]);
});

it('keeps every connection in its heartbeat slice when another one detaches', function () {
    $hub = new class(heartbeatSlices: 4) extends Hub
    {
        /**
         * Get the heartbeat slice of an attached connection.
         */
        public function sliceOf(Connection $connection): int
        {
            return $this->connections[$connection]['slice'];
        }
    };
    $connections = [];

    for ($i = 0; $i < 6; $i++) {
        [$connections[]] = fakeConnection();
        $hub->attach($connections[$i], ['news']);
    }

    $slices = array_map($hub->sliceOf(...), $connections);

    foreach ([0, 2, 4] as $i) {
        $hub->detach($connections[$i]);
    }

    foreach ([1, 3, 5] as $i) {
        expect($hub->sliceOf($connections[$i]))->toBe($slices[$i]);
    }

    expect($hub->connectionCount())->toBe(3);
});

it('keeps a reattached connection in its original heartbeat slice', function () {
    $hub = new Hub(heartbeatSlices: 2);
    [$first, $firstOutput] = fakeConnection();
    [$second, $secondOutput] = fakeConnection();
    $hub->attach($first, ['news']);
    $hub->heartbeatSlice();
    $hub->attach($second, ['news']);
    $hub->attach($second, ['other']);

    $hub->heartbeatSlice();
    expect($firstOutput())->toBe(Frame::comment())->and($secondOutput())->toBe('');

    $hub->heartbeatSlice();
    expect($secondOutput())->toBe(Frame::comment());
});

it('reads the latest dispatched position when each heartbeat slice runs', function () {
    $hub = new Hub(heartbeatSlices: 2);
    [$first, $firstOutput] = fakeConnection();
    [$second, $secondOutput] = fakeConnection();
    $hub->attach($first, ['news']);
    $hub->heartbeatSlice();
    $hub->attach($second, ['news']);

    $hub->heartbeatSlice();
    $position = new EventId(10, 4);
    $hub->dispatch(new Event('other', 'updated', id: $position));
    $hub->heartbeatSlice();

    expect($firstOutput())->toBe(Frame::comment())->and($secondOutput())->toBe(Frame::heartbeat($position));
});

it('closes a failing connection and still writes to the rest of its slice', function () {
    $hub = new Hub(heartbeatSlices: 2);
    [$broken, , $stream] = fakeConnection();
    [$earlier, $earlierOutput] = fakeConnection();
    [$healthy, $healthyOutput] = fakeConnection();
    $reason = null;
    $broken->onClose(function (string $value) use (&$reason): void {
        $reason = $value;
    });
    $stream->on('data', fn () => throw new RuntimeException('Heartbeat write failed.'));
    $hub->attach($earlier, ['news']);
    $hub->heartbeatSlice();
    $hub->attach($broken, ['news']);
    $hub->attach($healthy, ['news']);
    $hub->heartbeatSlice();
    expect($earlierOutput())->toBe(Frame::comment());

    $hub->heartbeatSlice();

    expect($reason)->toBe('error')
        ->and($hub->connectionCount())->toBe(2)
        ->and($healthyOutput())->toBe(Frame::comment())
        ->and($earlierOutput())->toBe(Frame::comment());
});

it('never schedules the first heartbeat earlier than half a rotation for any slice count', function (int $slices) {
    $hub = new class(heartbeatSlices: $slices) extends Hub
    {
        /**
         * Get the number of ticks until the given connection's slice is sent.
         */
        public function ticksUntil(Connection $connection): int
        {
            $count = count($this->heartbeatBuckets);

            return (($this->connections[$connection]['slice'] - $this->nextHeartbeatSlice) % $count + $count) % $count + 1;
        }
    };

    for ($tick = 0; $tick < $slices * 2; $tick++) {
        [$connection] = fakeConnection();
        $hub->attach($connection, ['news']);

        expect($hub->ticksUntil($connection))->toBeGreaterThanOrEqual((int) ceil($slices / 2))->toBeLessThanOrEqual($slices);

        $hub->heartbeatSlice();
    }
})->with([2, 3, 5, 25, 50]);

it('removes a closed connection from its heartbeat slice', function () {
    $hub = new class(heartbeatSlices: 3) extends Hub
    {
        /**
         * Get the number of connections in each heartbeat bucket.
         */
        public function bucketSizes(): array
        {
            return array_map('count', $this->heartbeatBuckets);
        }
    };
    [$closed] = fakeConnection();
    [$detached] = fakeConnection();
    [$kept] = fakeConnection();
    $hub->attach($closed, ['news']);
    $hub->attach($detached, ['news']);
    $hub->attach($kept, ['news']);
    expect(array_sum($hub->bucketSizes()))->toBe(3);

    $closed->close();
    $hub->detach($detached);

    expect(array_sum($hub->bucketSizes()))->toBe(1)->and($hub->connectionCount())->toBe(1);
});

it('refuses a hub without heartbeat slices', function () {
    new Hub(heartbeatSlices: 0);
})->throws(InvalidArgumentException::class);
