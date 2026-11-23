<?php

use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SseReader;
use Arttecs\Murmur\Tests\TestCase;
use Symfony\Component\Process\Process;

use function React\Async\delay;

pest()->extend(TestCase::class);

it('delivers a laravel broadcast to a browser subscription end to end', function () {
    $h = ServerHarness::start();
    config(['murmur.publish_url' => $h->url(''), 'murmur.public_url' => $h->url(''), 'murmur.stream_cursor_margin_ms' => 0]);
    $stream = Murmur::stream('private-chat.42');
    $r = SseReader::open($stream->url());
    $r->next();
    publishFromSubprocess($h, 'private-chat.42', 'token', ['text' => 'Olá']);
    $frame = $r->next();
    expect($frame['event'])->toBe('token');
    expect(json_decode($frame['data'], true)['data'])->toBe(['text' => 'Olá']);
});

it('reports an oversized publish as a publish failure', function () {
    $h = ServerHarness::start(['maxPayloadBytes' => 64]);
    $process = runPublishProcess([
        __DIR__.'/../../Support/publish-oversized.php',
        $h->url(''),
        config('murmur.secret'),
    ]);

    expect($process->getExitCode())->toBe(0, $process->getErrorOutput());
});

/**
 * Send a signed publish request while the test server handles HTTP requests.
 */
function publishFromSubprocess(ServerHarness $h, string $topic, string $event, mixed $data): void
{
    $process = runPublishProcess([
        __DIR__.'/../../Support/publish.php',
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
