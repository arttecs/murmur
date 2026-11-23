<?php

use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\MurmurManager;
use Arttecs\Murmur\Server\Connection;
use Arttecs\Murmur\Tests\Support\FakePublisher;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SseReader;
use Arttecs\Murmur\Tests\TestCase;
use React\Stream\ThroughStream;

pest()->extend(TestCase::class)->in('Feature/*Test.php');

pest()->in('Feature')->afterEach(function () {
    SseReader::closeAll();
    ServerHarness::stopAll();
});

/**
 * Bind a publisher that records the events sent by the application.
 */
function fakePublisher(): FakePublisher
{
    $publisher = new FakePublisher;

    app()->instance(Publisher::class, $publisher);
    app()->forgetInstance(MurmurManager::class);
    Murmur::clearResolvedInstance(MurmurManager::class);

    return $publisher;
}

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
