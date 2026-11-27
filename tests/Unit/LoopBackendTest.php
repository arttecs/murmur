<?php

use Arttecs\Murmur\Server\LoopBackend;
use React\EventLoop\ExtEventLoop;
use React\EventLoop\ExtEvLoop;
use React\EventLoop\ExtUvLoop;
use React\EventLoop\StreamSelectLoop;

it('describes the stream select limit and rejects configurations above it', function () {
    $backend = new LoopBackend(StreamSelectLoop::class);

    expect($backend->notice())->toContain('1000', 'stream_select', 'ext-uv', 'ext-ev', 'ext-event');
    $backend->validate(1000);
    expect(fn () => $backend->validate(1001))->toThrow(InvalidArgumentException::class, '1000');
});

it('allows more than 1000 sockets without a notice on extension loops', function (string $class) {
    $backend = new LoopBackend($class);

    $backend->validate(1001);
    expect($backend->notice())->toBeNull();
})->with([ExtUvLoop::class, ExtEvLoop::class, ExtEventLoop::class]);
