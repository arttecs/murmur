<?php

use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\Metrics\MetricsRecorder;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Server\Server;
use Arttecs\Murmur\Server\ServerConfig;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SseReader;
use React\EventLoop\Loop;
use React\Http\Browser;
use React\Socket\SocketServer;
use Symfony\Component\Process\Process;

use function React\Async\await;

function metricsServingProcess(int $port, array $env): Process
{
    return new Process([PHP_BINARY, '-d', 'error_log=', dirname(__DIR__).'/Support/metrics-artisan.php', 'murmur:serve', "--port={$port}"], env: $env + [
        'MURMUR_DRIVER' => 'http',
        'MURMUR_HOST' => '127.0.0.1',
        'MURMUR_SECRET' => config('murmur.secret'),
        'MURMUR_METRICS_STORE' => 'fake',
        'MURMUR_METRICS_INTERVAL' => '0.06',
        'MURMUR_METRICS_TIMEOUT' => '2',
        'MURMUR_METRICS_TTL' => '60',
        'MURMUR_NODE' => '',
        'MURMUR_METRICS_ONLY' => '',
    ], timeout: 5);
}

function metricsTrace(string $file, ?string $phase = null): array
{
    $entries = array_filter(array_map(json_decode(...), file($file, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES)));

    return array_values(array_filter($entries, fn (object $entry): bool => $phase === null || $entry->phase === $phase));
}

it('writes immediately after listening, then records a filtered snapshot on the interval', function () {
    $port = ServerHarness::availablePort();
    $file = tempnam(sys_get_temp_dir(), 'murmur-recorder-');
    $process = metricsServingProcess($port, [
        'MURMUR_TEST_METRICS_FILE' => $file,
        'MURMUR_METRICS_INTERVAL' => '0.8',
        'MURMUR_METRICS_ONLY' => 'murmur_connections_opened_total',
    ]);

    try {
        $process->start();
        waitForMetricsCondition(fn () => str_contains($process->getOutput(), 'Murmur is listening'), $process);
        expect($process->getOutput())->toContain(":{$port}");
        $listening = hrtime(true);
        waitForMetricsCondition(fn () => count(metricsTrace($file, 'saved')) >= 1, $process);
        $elapsed = hrtime(true);
        expect(($elapsed - $listening) / 1_000_000_000)->toBeLessThan(0.65);
        $reader = SseReader::open("http://127.0.0.1:{$port}/sse?topics=news");
        expect($reader->next()['retry'])->toBe('3000');
        waitForMetricsCondition(fn () => count(metricsTrace($file, 'saved')) >= 2, $process);
        [$first, $second] = metricsTrace($file, 'saved');

        expect((hrtime(true) - $elapsed) / 1_000_000_000)->toBeGreaterThan(0.3);
        expect($first->node)->toBe(gethostname().":{$port}");
        expect($first->samples)->toHaveCount(1);
        expect($first->samples[0]->name)->toBe('murmur_connections_opened_total');
        expect($first->samples[0]->value)->toBe(0);
        expect($second->samples[0]->value)->toBe(1);
    } finally {
        $process->stop(0.5);
        unlink($file);
    }
});

it('continues accepting requests during a slow write and skips overlapping ticks', function () {
    $port = ServerHarness::availablePort();
    $file = tempnam(sys_get_temp_dir(), 'murmur-recorder-');
    $process = metricsServingProcess($port, [
        'MURMUR_TEST_METRICS_FILE' => $file,
        'MURMUR_TEST_METRICS_MODE' => 'slow',
    ]);

    try {
        $process->start();
        waitForMetricsCondition(fn () => count(metricsTrace($file, 'start')) >= 1, $process);
        $response = await((new Browser)->withTimeout(0.2)->get("http://127.0.0.1:{$port}/health"));
        expect($response->getStatusCode())->toBe(200);
        expect(metricsTrace($file, 'saved'))->toBe([]);
        waitForMetricsCondition(fn () => count(metricsTrace($file)) >= 3, $process);

        $events = metricsTrace($file);
        expect(array_slice(array_column($events, 'phase'), 0, 3))->toBe(['start', 'saved', 'start']);
        expect($events[0]->pid)->toBe($events[1]->pid)->not->toBe($events[2]->pid);
    } finally {
        $process->stop(0.5);
        unlink($file);
    }
});

it('reports child failures and keeps recording', function (string $mode, string $message) {
    $port = ServerHarness::availablePort();
    $file = tempnam(sys_get_temp_dir(), 'murmur-recorder-');
    $process = metricsServingProcess($port, [
        'MURMUR_TEST_METRICS_FILE' => $file,
        'MURMUR_TEST_METRICS_MODE' => $mode,
        'MURMUR_METRICS_TIMEOUT' => '0.4',
        'MURMUR_METRICS_ONLY' => 'murmur_errors_total',
    ]);

    try {
        $process->start();
        waitForMetricsCondition(fn () => count(metricsTrace($file, 'saved')) >= 1, $process);
        expect($process->getErrorOutput())->toContain($message);
        expect($process->getErrorOutput())->not->toContain('sensitive store exception');

        $failed = metricsTrace($file, 'start')[0];
        $success = metricsTrace($file, 'saved')[0];
        expect($success->samples[0]->value)->toBe(1);
        expect($success->pid)->not->toBe($failed->pid);
        expect(posix_kill($failed->pid, 0))->toBeFalse();
    } finally {
        $process->stop(0.5);
        unlink($file);
    }
})->with([['fail-first', 'Murmur metrics recorder exited'], ['hang', 'Murmur metrics recorder timed out']])
    ->skip(! extension_loaded('posix'), 'Checking that the child was reaped requires ext-posix.');

it('terminates and reaps a running writer when serving shuts down', function () {
    $file = tempnam(sys_get_temp_dir(), 'murmur-recorder-');
    $port = ServerHarness::availablePort();
    $process = metricsServingProcess($port, [
        'MURMUR_TEST_METRICS_FILE' => $file,
        'MURMUR_TEST_METRICS_MODE' => 'hang',
        'MURMUR_METRICS_TIMEOUT' => '10',
    ]);

    try {
        $process->start();
        waitForMetricsCondition(fn () => count(metricsTrace($file, 'waiting')) >= 1, $process);
        $child = metricsTrace($file, 'start')[0]->pid;
        $process->signal(SIGTERM);
        waitForMetricsCondition(fn () => ! $process->isRunning());

        expect($process->getExitCode())->toBe(0, $process->getErrorOutput());
        expect(posix_kill($child, 0))->toBeFalse();
        expect(array_column(metricsTrace($file), 'phase'))->toBe(['start', 'waiting']);
    } finally {
        $process->stop(0.5);
        unlink($file);
    }
})->skip(! extension_loaded('pcntl') || ! extension_loaded('posix'), 'Signal handling requires ext-pcntl and ext-posix.');

it('does not load the process dependency when metrics are disabled', function () {
    $process = metricsServingProcess(ServerHarness::availablePort(), [
        'MURMUR_METRICS_STORE' => '',
        'MURMUR_TEST_METRICS_DISABLED' => '1',
    ]);
    $process->mustRun();

    expect($process->getOutput())->toContain('Recorder inactive')->not->toContain('Recorder loaded');
});

it('releases listening and accepted sockets while the writer is running', function () {
    $file = tempnam(sys_get_temp_dir(), 'murmur-recorder-');
    $port = ServerHarness::availablePort();
    $server = new Server(new ServerConfig(secret: config('murmur.secret'), port: $port), new MemoryEventLog);
    config(['murmur.metrics.store' => 'cache']);
    $command = 'env MURMUR_METRICS_STORE=fake MURMUR_TEST_METRICS_MODE=hang MURMUR_TEST_METRICS_FILE='.escapeshellarg($file)
        .' '.escapeshellarg(PHP_BINARY).' '.escapeshellarg(dirname(__DIR__).'/Support/metrics-artisan.php').' murmur:record-metrics';
    $writer = new MetricsRecorder(Loop::get(), Murmur::metricsConfiguration(), 'test-node', $command);
    $other = null;
    $descriptors = array_map(fn () => fopen('/dev/null', 'r'), range(1, 16));

    try {
        await($server->listen());
        $reader = SseReader::open($server->address().'/sse?topics=news');
        expect($reader->next()['retry'])->toBe('3000');
        $writer->start($server->metrics());
        waitForMetricsCondition(fn () => count(metricsTrace($file, 'waiting')) >= 1);
        $child = metricsTrace($file, 'start')[0]->pid;
        $server->close();

        expect(fn () => $reader->next(0.15))->toThrow(RuntimeException::class, 'The SSE stream closed');
        $other = new SocketServer("127.0.0.1:{$port}");
        expect(posix_kill($child, 0))->toBeTrue();
    } finally {
        $server->close();
        await($writer->stop());
        $other?->close();
        foreach ($descriptors as $descriptor) {
            fclose($descriptor);
        }
        unlink($file);
    }
})->skip(! extension_loaded('posix'), 'Checking child liveness requires ext-posix.');

it('explains invalid metrics settings before opening a listener', function () {
    config(['murmur.metrics.store' => 'cache', 'murmur.metrics.interval' => 0]);

    $listener = new SocketServer('127.0.0.1:0');

    try {
        $this->artisan('murmur:serve', ['--port' => parse_url($listener->getAddress(), PHP_URL_PORT)])
            ->expectsOutputToContain('metrics.interval must be a positive, finite number')
            ->assertExitCode(1);
    } finally {
        $listener->close();
    }
});

it('refuses to serve when the host cannot launch the metrics recorder', function () {
    $port = ServerHarness::availablePort();
    $process = new Process([
        PHP_BINARY, '-d', 'error_log=', '-d', 'disable_functions=proc_open',
        dirname(__DIR__).'/Support/metrics-artisan.php', 'murmur:serve', "--port={$port}",
    ], env: [
        'MURMUR_SECRET' => config('murmur.secret'),
        'MURMUR_METRICS_STORE' => 'cache',
        'MURMUR_METRICS_INTERVAL' => '0.01',
        'MURMUR_TEST_METRICS_DISABLED' => '1',
    ], timeout: 3);

    $process->run();

    expect($process->getExitCode())->toBe(1);
    expect($process->getOutput())->not->toContain('Murmur is listening');
    expect($process->getOutput().$process->getErrorOutput())->toContain('Murmur metrics recording requires proc_open() on Linux or macOS.');
});

it('refuses to start without bash or a descriptor directory', function (string $missing, string $message) {
    config(['murmur.metrics.store' => 'cache']);
    expect(fn () => new class($missing) extends MetricsRecorder
    {
        public function __construct(protected string $missing)
        {
            parent::__construct(Loop::get(), Murmur::metricsConfiguration(), 'test-node', 'php artisan murmur:record-metrics');
        }

        protected function bashBinary(): ?string
        {
            return $this->missing === 'bash' ? null : parent::bashBinary();
        }

        protected function descriptorsListable(): bool
        {
            return $this->missing !== 'descriptors';
        }
    })->toThrow(InvalidArgumentException::class, $message);
})->with([
    ['bash', 'Murmur metrics recording requires Bash.'],
    ['descriptors', 'Murmur metrics recording requires a readable /proc/self/fd or /dev/fd.'],
]);

it('resolves bash to an absolute executable path', function () {
    config(['murmur.metrics.store' => 'cache']);
    $recorder = new MetricsRecorder(Loop::get(), Murmur::metricsConfiguration(), 'test-node', 'php artisan murmur:record-metrics');
    $bash = (new ReflectionProperty($recorder, 'bash'))->getValue($recorder);

    expect($bash)->toStartWith('/')->toEndWith('/bash');
    expect(is_executable($bash))->toBeTrue();
});
