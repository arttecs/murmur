<?php

use Arttecs\Murmur\Server\Connection;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SseReader;
use React\Stream\ThroughStream;

pest()->in('Feature')->afterEach(function () {
    SseReader::closeAll();
    ServerHarness::stopAll();
});

/**
 * Create a connection and capture the frames it writes.
 *
 * @return array{Connection, callable(): string, ThroughStream}
 */
function fakeConnection(): array
{
    $stream = new ThroughStream;
    $output = '';
    $stream->on('data', function (string $data) use (&$output): void {
        $output .= $data;
    });

    return [new Connection($stream), function () use (&$output): string {
        return $output;
    }, $stream];
}
