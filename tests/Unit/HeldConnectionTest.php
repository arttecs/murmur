<?php

use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Http\HeldConnection;
use React\Stream\ThroughStream;

it('writes replay before held frames and sends subsequent frames immediately', function () {
    $stream = new ThroughStream;
    $frames = [];
    $stream->on('data', function (string $frame) use (&$frames): void {
        $frames[] = $frame;
    });
    $connection = new HeldConnection($stream);

    $connection->send("id: 10-1\ndata: older\n\n");
    $connection->send("id: 10-2\ndata: duplicate\n\n");
    $connection->send("id: 10-3\ndata: queued\n\n");
    expect($frames)->toBe([]);

    $connection->sendImmediately("id: 10-2\ndata: replay\n\n");
    expect($frames)->toBe(["id: 10-2\ndata: replay\n\n"]);
    $connection->release(new EventId(10, 2));
    expect($frames)->toBe(["id: 10-2\ndata: replay\n\n", "id: 10-3\ndata: queued\n\n"]);

    $connection->send("id: 10-4\ndata: live\n\n");
    expect($frames)->toBe(["id: 10-2\ndata: replay\n\n", "id: 10-3\ndata: queued\n\n", "id: 10-4\ndata: live\n\n"]);
});

it('drops held frames when the connection closes before release', function () {
    $stream = new ThroughStream;
    $frames = [];
    $stream->on('data', function (string $frame) use (&$frames): void {
        $frames[] = $frame;
    });
    $connection = new HeldConnection($stream);
    $connection->send('held');

    $connection->close();
    $connection->release(null);

    expect($connection->isClosed())->toBeTrue();
    expect($frames)->toBe([]);
});

it('evicts a held connection when its queued bytes exceed the limit', function () {
    $stream = new ThroughStream;
    $connection = new HeldConnection($stream, maxBufferBytes: 10);

    $connection->send('123456');
    $connection->send('7890');
    expect($connection->isClosed())->toBeFalse();

    $connection->send('1');
    expect($connection->isClosed())->toBeTrue();
    expect($stream->isWritable())->toBeFalse();
});
