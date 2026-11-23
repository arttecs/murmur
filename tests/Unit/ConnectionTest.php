<?php

use Arttecs\Murmur\Server\Connection;
use React\Stream\ThroughStream;

it('closes a connection whose client stopped reading', function () {
    $stream = new ThroughStream;
    $stream->pause();
    $connection = new Connection($stream, maxBufferBytes: 10);
    $connection->send(str_repeat('x', 11));
    expect($connection->isClosed())->toBeTrue();
});

it('runs close listeners once', function () {
    [$a, , $stream] = fakeConnection();
    $calls = 0;
    $a->onClose(function () use (&$calls) {
        $calls++;
    });
    $a->close();
    $stream->close();
    expect($calls)->toBe(1);
});
