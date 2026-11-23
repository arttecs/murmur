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
use Symfony\Component\Process\Process;

use function React\Async\delay;

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

/**
 * Send a signed publish request while the test server handles HTTP requests.
 */
function publishFromSubprocess(ServerHarness $h, string $topic, string $event, mixed $data): void
{
    $process = runPublishProcess([
        __DIR__.'/Support/publish.php',
        $h->url('/publish'),
        config('murmur.secret'),
        json_encode(compact('topic', 'event', 'data'), JSON_THROW_ON_ERROR),
    ]);

    expect($process->getExitCode())->toBe(0, $process->getErrorOutput());
}

/**
 * Run a PHP subprocess without blocking the test server's event loop.
 *
 * @param  list<string>  $arguments
 */
function runPublishProcess(array $arguments): Process
{
    $process = new Process([PHP_BINARY, ...$arguments], timeout: 5);
    $process->start();

    while ($process->isRunning()) {
        $process->checkTimeout();
        delay(0.01);
    }

    return $process;
}
