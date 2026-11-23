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

it('counts backpressure across writes and tolerates the exact limit', function () {
    $stream = new ThroughStream;
    $stream->pause();
    $connection = new Connection($stream, maxBufferBytes: 10);

    $connection->send(str_repeat('x', 6));
    $connection->send(str_repeat('x', 4));
    expect($connection->isClosed())->toBeFalse();

    $connection->send('x');
    expect($connection->isClosed())->toBeTrue();
});

it('forgets the backpressure once the client drains', function () {
    $stream = new ThroughStream;
    $stream->pause();
    $connection = new Connection($stream, maxBufferBytes: 10);

    $connection->send(str_repeat('x', 8));
    $stream->resume();
    $stream->pause();
    $connection->send(str_repeat('x', 8));

    expect($connection->isClosed())->toBeFalse();
});

it('ignores frames sent after closing', function () {
    [$connection, $output] = fakeConnection();
    $connection->close();
    $connection->send('late');

    expect($output())->toBe('');
});

it('calls a close listener immediately when already closed', function () {
    [$connection] = fakeConnection();
    $connection->close();
    $calls = 0;
    $connection->onClose(function () use (&$calls) {
        $calls++;
    });

    expect($calls)->toBe(1);
});
