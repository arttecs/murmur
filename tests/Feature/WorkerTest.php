<?php

use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\Server\ErrorReporter;
use Arttecs\Murmur\Server\Metrics;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SseReader;
use Arttecs\Murmur\Worker\Worker;
use Psr\Log\NullLogger;
use React\EventLoop\Loop;
use React\Http\Browser;
use React\Socket\SocketServer;
use Symfony\Component\Process\Process;

use function React\Async\await;
use function React\Async\delay;

it('records the first snapshot as soon as the server listens', function () {
    $port = ServerHarness::availablePort();
    $file = tempnam(sys_get_temp_dir(), 'murmur-recorder-');
    $process = workerServingProcess($port, [
        'MURMUR_TEST_METRICS_FILE' => $file,
        'MURMUR_METRICS_INTERVAL' => '30',
    ]);

    try {
        $process->start();
        waitForMetricsCondition(fn () => count(metricsTrace($file, 'saved')) === 1, $process);

        expect($process->getOutput())->toContain("Murmur is listening on http://127.0.0.1:{$port}");
        $samples = array_column(metricsTrace($file, 'saved')[0]->samples, 'value', 'name');
        expect($samples['murmur_uptime_seconds'])->toBeGreaterThan(0);
    } finally {
        $process->stop(2);
        unlink($file);
    }
});

it('records filtered snapshots on the interval with the same writer', function () {
    $port = ServerHarness::availablePort();
    $file = tempnam(sys_get_temp_dir(), 'murmur-recorder-');
    $process = workerServingProcess($port, [
        'MURMUR_TEST_METRICS_FILE' => $file,
        'MURMUR_METRICS_INTERVAL' => '1.25',
        'MURMUR_METRICS_ONLY' => 'murmur_connections_opened_total',
    ]);
    $reader = null;

    try {
        $process->start();
        waitForMetricsCondition(fn () => count(metricsTrace($file, 'saved')) >= 1, $process);
        $reader = SseReader::open("http://127.0.0.1:{$port}/sse?topics=news");
        expect($reader->next()['retry'])->toBe('3000');

        waitForMetricsCondition(fn () => in_array(1, array_map(fn (object $entry) => $entry->samples[0]->value, metricsTrace($file, 'saved')), true), $process);
        $entries = metricsTrace($file, 'saved');
        $first = $entries[0];
        $last = end($entries);

        expect(array_unique(array_column($entries, 'pid')))->toHaveCount(1);
        expect($first->node)->toBe(gethostname().":{$port}");
        expect($first->samples)->toHaveCount(1);
        expect($first->samples[0]->name)->toBe('murmur_connections_opened_total');
        expect($first->samples[0]->value)->toBe(0);
        expect($last->samples[0]->value)->toBe(1);
    } finally {
        $reader?->close();
        $process->stop(2);
        unlink($file);
    }
});

it('keeps HTTP responsive while a write is pending and resumes without queued ticks after a single timeout', function () {
    $port = ServerHarness::availablePort();
    $file = tempnam(sys_get_temp_dir(), 'murmur-recorder-');
    $process = workerServingProcess($port, [
        'MURMUR_TEST_METRICS_FILE' => $file,
        'MURMUR_TEST_METRICS_MODE' => 'gate',
        'MURMUR_METRICS_TIMEOUT' => '0.2',
        'MURMUR_METRICS_ONLY' => 'murmur_errors_total,murmur_connections_opened_total',
    ]);
    $reader = null;

    try {
        $process->start();
        waitForMetricsCondition(fn () => count(metricsTrace($file, 'waiting')) === 1, $process);
        waitForMetricsCondition(fn () => str_contains($process->getErrorOutput(), 'Murmur metrics recorder timed out'), $process);
        delay(0.5);

        $response = await((new Browser)->withTimeout(4)->get("http://127.0.0.1:{$port}/health"));
        expect($response->getStatusCode())->toBe(200);
        $reader = SseReader::open("http://127.0.0.1:{$port}/sse?topics=news");
        expect($reader->next()['retry'])->toBe('3000');
        expect(array_column(metricsTrace($file), 'phase'))->toBe(['start', 'waiting']);
        touch($file.'.release');

        waitForMetricsCondition(fn () => count(metricsTrace($file, 'saved')) >= 2, $process);
        $events = metricsTrace($file);
        expect(array_slice(array_column($events, 'phase'), 0, 4))->toBe(['start', 'waiting', 'saved', 'start']);
        expect(array_unique(array_column($events, 'pid')))->toHaveCount(1);
        expect(array_column(metricsTrace($file, 'saved')[1]->samples, 'value', 'name'))->toMatchArray([
            'murmur_errors_total' => 1,
            'murmur_connections_opened_total' => 1,
        ]);
    } finally {
        $reader?->close();
        $process->stop(2);
        if (is_file($file.'.release')) {
            unlink($file.'.release');
        }
        unlink($file);
    }
});

it('reports failed acknowledgements and keeps recording with the same writer', function () {
    $port = ServerHarness::availablePort();
    $file = tempnam(sys_get_temp_dir(), 'murmur-recorder-');
    $process = workerServingProcess($port, [
        'MURMUR_TEST_METRICS_FILE' => $file,
        'MURMUR_TEST_METRICS_MODE' => 'fail-first',
        'MURMUR_METRICS_ONLY' => 'murmur_errors_total',
    ]);

    try {
        $process->start();
        waitForMetricsCondition(fn () => count(metricsTrace($file, 'saved')) >= 1, $process);

        expect($process->getErrorOutput())->toContain('Murmur could not write the metrics snapshot.')
            ->not->toContain('sensitive store exception', 'The test exception handler failed.');
        [$failed, $reported, $next, $saved] = metricsTrace($file);
        expect([$failed->phase, $reported->phase, $next->phase, $saved->phase])->toBe(['start', 'reported', 'start', 'saved']);
        expect($reported->message)->toBe('sensitive store exception');
        expect($saved->samples[0]->value)->toBe(1);
        expect($saved->pid)->toBe($failed->pid);
    } finally {
        $process->stop(2);
        unlink($file);
    }
});

it('forwards noisy store output and diagnostics to stderr without acknowledgement errors', function () {
    $file = tempnam(sys_get_temp_dir(), 'murmur-recorder-');
    $process = workerServingProcess(ServerHarness::availablePort(), [
        'MURMUR_TEST_METRICS_FILE' => $file,
        'MURMUR_TEST_METRICS_MODE' => 'noisy',
        'MURMUR_METRICS_ONLY' => 'murmur_errors_total',
    ]);

    try {
        $process->start();
        waitForMetricsCondition(fn () => count(metricsTrace($file, 'saved')) >= 2, $process);
        [$first, $second] = metricsTrace($file, 'saved');

        expect($process->getErrorOutput())->toContain('store output', 'writer diagnostic line', 'store echo', 'store dump')->not->toContain('invalid acknowledgement');
        expect($process->getOutput())->not->toContain('store output');
        expect($second->samples[0]->value)->toBe(0);
        expect($second->pid)->toBe($first->pid);
    } finally {
        $process->stop(2);
        unlink($file);
    }
});

it('keeps recording when a store writes output without a line break', function () {
    $file = tempnam(sys_get_temp_dir(), 'murmur-recorder-');
    $process = workerServingProcess(ServerHarness::availablePort(), [
        'MURMUR_TEST_METRICS_FILE' => $file,
        'MURMUR_TEST_METRICS_MODE' => 'stray',
        'MURMUR_METRICS_ONLY' => 'murmur_errors_total',
    ]);

    try {
        $process->start();
        waitForMetricsCondition(fn () => count(metricsTrace($file, 'saved')) >= 3, $process);
        $saved = metricsTrace($file, 'saved');

        expect($process->getErrorOutput())->toContain('direct noise')->not->toContain('invalid acknowledgement');
        expect($saved[2]->samples[0]->value)->toBe(0);
        expect($saved[2]->pid)->toBe($saved[0]->pid);
    } finally {
        $process->stop(2);
        unlink($file);
    }
});

it('does not queue more snapshots while the writer input pipe is blocked', function () {
    config(['murmur.metrics.store' => 'cache', 'murmur.metrics.interval' => 0.02, 'murmur.metrics.timeout' => 0.4]);
    $loop = Loop::get();
    $command = escapeshellarg(PHP_BINARY).' -r '.escapeshellarg('fwrite(fopen("php://fd/3", "w"), "ok\n"); sleep(10);');
    $writer = new Worker($loop, Murmur::metricsConfiguration(), 'test-node', $command);
    $metrics = new Metrics;
    $snapshots = 0;
    $metrics->gauge('murmur_uptime_seconds', str_repeat('x', 524288), function () use (&$snapshots): int {
        return ++$snapshots;
    });

    $errors = 0;
    $counter = function () use (&$errors): void {
        $errors++;
    };
    $logger = new NullLogger;
    ErrorReporter::countUsing($counter);
    ErrorReporter::logUsing($logger);

    try {
        $writer->launch();
        $writer->start($metrics);
        waitForMetricsCondition(function () use (&$errors): bool {
            return $errors >= 2;
        });
        expect($snapshots)->toBe(1);
        expect($writer->running())->toBeTrue();
    } finally {
        await($writer->stop());
        ErrorReporter::forget($counter, $logger);
    }

    expect($writer->running())->toBeFalse();
});

it('exits after its parent is killed while a store write is pending', function () {
    $file = tempnam(sys_get_temp_dir(), 'murmur-recorder-');
    $process = workerServingProcess(ServerHarness::availablePort(), [
        'MURMUR_TEST_METRICS_FILE' => $file,
        'MURMUR_TEST_METRICS_MODE' => 'gate',
        'MURMUR_METRICS_TIMEOUT' => '10',
    ]);
    $child = null;

    try {
        $process->start();
        waitForMetricsCondition(fn () => count(metricsTrace($file, 'waiting')) === 1, $process);
        $child = metricsTrace($file, 'start')[0]->pid;
        $process->signal(SIGKILL);
        waitForMetricsCondition(fn () => ! $process->isRunning());
        waitForMetricsCondition(fn () => ! posix_kill($child, 0));

        expect(array_column(metricsTrace($file), 'phase'))->toBe(['start', 'waiting']);
    } finally {
        $process->stop(2);
        if ($child !== null && posix_kill($child, 0)) {
            posix_kill($child, SIGKILL);
        }
        unlink($file);
    }
})->skip(! extension_loaded('pcntl') || ! extension_loaded('posix'), 'Checking for an orphan requires ext-pcntl and ext-posix.');

it('reports writer death once and keeps accepting SSE connections without respawning', function () {
    $port = ServerHarness::availablePort();
    $file = tempnam(sys_get_temp_dir(), 'murmur-recorder-');
    $process = workerServingProcess($port, [
        'MURMUR_TEST_METRICS_FILE' => $file,
        'MURMUR_TEST_METRICS_MODE' => 'gate',
        'MURMUR_METRICS_TIMEOUT' => '0.2',
    ]);
    $reader = null;

    try {
        $process->start();
        waitForMetricsCondition(fn () => count(metricsTrace($file, 'waiting')) >= 1, $process);
        $child = metricsTrace($file, 'start')[0]->pid;
        waitForMetricsCondition(fn () => str_contains($process->getErrorOutput(), 'Murmur metrics recorder timed out'), $process);
        posix_kill($child, SIGKILL);
        waitForMetricsCondition(fn () => str_contains($process->getErrorOutput(), 'worker recording has stopped'), $process);

        $reader = SseReader::open("http://127.0.0.1:{$port}/sse?topics=news");
        expect($reader->next()['retry'])->toBe('3000');
        delay(0.3);
        expect(array_column(metricsTrace($file), 'phase'))->toBe(['start', 'waiting']);
        expect(substr_count($process->getErrorOutput(), 'worker recording has stopped'))->toBe(1);
        expect($process->getErrorOutput())->toContain('signal '.SIGKILL);
        expect(posix_kill($child, 0))->toBeFalse();
    } finally {
        $reader?->close();
        $process->stop(2);
        unlink($file);
    }
})->skip(! extension_loaded('posix') || ! extension_loaded('pcntl'), 'Killing and checking the writer requires ext-posix and ext-pcntl.');

it('reaps the writer on signal-driven shutdown', function (string $mode, string $phase, array $phases, bool $group) {
    if ($group && ! extension_loaded('posix')) {
        $this->markTestSkipped('Sending a process group signal requires ext-posix.');
    }

    $file = tempnam(sys_get_temp_dir(), 'murmur-recorder-');
    $process = workerServingProcess(ServerHarness::availablePort(), [
        'MURMUR_TEST_METRICS_FILE' => $file,
        'MURMUR_TEST_METRICS_MODE' => $mode,
        'MURMUR_TEST_METRICS_SESSION' => $group ? '1' : '',
        'MURMUR_TEST_METRICS_SHUTDOWN_TEST' => $mode === 'gate' ? $file : '',
        'MURMUR_METRICS_INTERVAL' => '20',
        'MURMUR_METRICS_TIMEOUT' => '10',
    ]);

    try {
        $process->start();
        waitForMetricsCondition(fn () => count(metricsTrace($file, $phase)) === 1, $process);
        $child = metricsTrace($file, 'start')[0]->pid;
        if ($group) {
            expect(posix_kill(-$process->getPid(), SIGINT))->toBeTrue();
        } else {
            $process->signal(SIGTERM);
        }

        waitForMetricsCondition(fn () => ! $process->isRunning());

        expect($process->getExitCode())->toBe(0);
        expect($process->getErrorOutput())->toBe('');
        if (extension_loaded('posix')) {
            expect(posix_kill($child, 0))->toBeFalse();
        }

        expect(array_column(metricsTrace($file), 'phase'))->toBe($phases);
    } finally {
        $process->stop(2);
        if (is_file($file.'.release')) {
            unlink($file.'.release');
        }
        unlink($file);
    }
})->with([
    'idle on SIGTERM' => ['', 'saved', ['start', 'saved'], false],
    'mid-write on group SIGINT' => ['gate', 'waiting', ['start', 'waiting', 'saved'], true],
    'ignores SIGTERM in the store' => ['hang', 'waiting', ['start', 'waiting'], false],
])->skip(! extension_loaded('pcntl'), 'Signal handling requires ext-pcntl.');

it('does not load the process dependency when metrics and lifecycle delivery are disabled', function () {
    $process = workerServingProcess(ServerHarness::availablePort(), [
        'MURMUR_METRICS_STORE' => '',
        'MURMUR_TEST_METRICS_DISABLED' => '1',
    ]);
    $process->mustRun();

    expect($process->getOutput())->toContain('Recorder inactive')->not->toContain('Recorder loaded');
});

it('releases listening and accepted sockets while the writer is running', function () {
    $file = tempnam(sys_get_temp_dir(), 'murmur-recorder-');
    $port = ServerHarness::availablePort();
    $process = workerServingProcess($port, [
        'MURMUR_TEST_METRICS_FILE' => $file,
        'MURMUR_TEST_METRICS_MODE' => 'hang',
        'MURMUR_TEST_METRICS_SOCKET_TEST' => $file,
        'MURMUR_METRICS_TIMEOUT' => '10',
    ]);
    $reader = null;
    $other = null;

    try {
        $process->start();
        waitForMetricsCondition(fn () => count(metricsTrace($file, 'waiting')) === 1, $process);
        $child = metricsTrace($file, 'start')[0]->pid;
        $reader = SseReader::open("http://127.0.0.1:{$port}/sse?topics=news");
        expect($reader->start()['retry'])->toBe('3000');
        touch($file.'.release');

        expect(fn () => $reader->next(4))->toThrow(RuntimeException::class, 'The SSE stream closed');
        $other = new SocketServer("127.0.0.1:{$port}");
        expect(posix_kill($child, 0))->toBeTrue();
    } finally {
        $reader?->close();
        $other?->close();
        $process->stop(2);
        if (is_file($file.'.release')) {
            unlink($file.'.release');
        }
        unlink($file);
    }
})->skip(! extension_loaded('posix') || ! extension_loaded('pcntl'), 'Checking child liveness requires ext-posix and ext-pcntl.');

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
    expect($process->getOutput().$process->getErrorOutput())->toContain('Murmur worker requires proc_open() on Linux or macOS.');
});

it('refuses to serve when the Artisan entry point cannot be resolved', function () {
    $process = workerServingProcess(ServerHarness::availablePort(), [
        'MURMUR_TEST_ARTISAN_BINARY' => '/nonexistent/murmur-artisan',
        'MURMUR_TEST_METRICS_DISABLED' => '1',
    ]);
    $process->run();

    expect($process->getExitCode())->toBe(1);
    expect($process->getOutput())->not->toContain('Murmur is listening');
    expect($process->getOutput().$process->getErrorOutput())->toMatch('/\A[^\r\n]*could not locate the Artisan entry point[^\r\n]*\n\z/');
});

it('recovers after an overlong line written to the acknowledgement pipe', function () {
    config(['murmur.metrics.store' => 'cache', 'murmur.metrics.interval' => 0.02]);
    $code = <<<'PHP'
        $ack = fopen('php://fd/3', 'w');
        fgets(STDIN);
        fwrite($ack, str_repeat('x', 131072)."\n");
        while (fgets(STDIN) !== false) {
            fwrite($ack, "ok\n");
        }
        PHP;
    $worker = new Worker(Loop::get(), Murmur::metricsConfiguration(), 'test-node', escapeshellarg(PHP_BINARY).' -r '.escapeshellarg($code));
    $metrics = new Metrics;
    $snapshots = 0;
    $metrics->gauge('murmur_uptime_seconds', 'Snapshot number', function () use (&$snapshots): int {
        return ++$snapshots;
    });
    $errors = 0;
    $counter = function () use (&$errors): void {
        $errors++;
    };
    $logger = new NullLogger;
    ErrorReporter::countUsing($counter);
    ErrorReporter::logUsing($logger);

    try {
        $worker->launch();
        $worker->start($metrics);
        waitForMetricsCondition(function () use (&$snapshots): bool {
            return $snapshots >= 3;
        });

        expect($errors)->toBe(1);
    } finally {
        await($worker->stop());
        ErrorReporter::forget($counter, $logger);
    }
});
