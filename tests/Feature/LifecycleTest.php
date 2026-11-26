<?php

use Arttecs\Murmur\Server\Auth\TokenIssuer;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SseReader;
use React\Http\Browser;
use React\Socket\SocketServer;

use function React\Async\await;

it('dispatches accepted connections in order outside the server with the exact close reason', function (string $reason) {
    if ($reason === 'shutdown' && ! extension_loaded('pcntl')) {
        $this->markTestSkipped('Signal-driven shutdown requires ext-pcntl.');
    }

    $port = ServerHarness::availablePort();
    $file = tempnam(sys_get_temp_dir(), 'murmur-lifecycle-');
    $node = $reason === 'client' ? 'edge.test:8080' : '';
    $process = workerServingProcess($port, [
        'MURMUR_METRICS_STORE' => '',
        'MURMUR_LIFECYCLE_EVENTS' => 'true',
        'MURMUR_LIFECYCLE_INTERVAL' => $reason === 'shutdown' ? '60' : '0.02',
        'MURMUR_TEST_LIFECYCLE_FILE' => $file,
        'MURMUR_TEST_LIFECYCLE_MODE' => $reason,
        'MURMUR_NODE' => $node,
        'MURMUR_MAX_BUFFER_BYTES' => $reason === 'slow_consumer' ? '512' : '1048576',
        'MURMUR_SHUTDOWN_SPREAD' => '0',
    ]);
    $root = "http://127.0.0.1:{$port}";
    $id = str_repeat('a', 32);
    $reader = null;

    try {
        $process->start();
        $pid = $process->getPid();
        waitForMetricsCondition(fn () => str_contains($process->getOutput(), 'Murmur is listening'), $process);
        expect(SseReader::open($root.'/sse?topics=private-rejected')->status())->toBe(401);
        expect(SseReader::open($root.'/sse?topics=a%20b')->status())->toBe(400);

        $topics = $reason === 'expired' ? ['news', 'private-news'] : ['news', 'notícias-界-😀'];
        $expiry = time() + 2;
        $query = ['topics' => implode(',', [...$topics, 'news']), 'connection' => $id];
        if ($reason === 'expired') {
            $query['token'] = (new TokenIssuer(config('murmur.secret'), fn () => $expiry - 2))->issue(['private-news'], 2);
        }
        if (in_array($reason, ['error', 'slow_consumer'], true)) {
            $query['lastEventId'] = '1-0';
        }

        $before = time();
        $reader = SseReader::open($root.'/sse?'.http_build_query($query));
        expect($reader->status())->toBe(200);
        expect($reader->start(replaying: in_array($reason, ['error', 'slow_consumer'], true))['retry'])->toBe('3000');

        if ($reason === 'client') {
            $reader->close();
        } elseif ($reason === 'shutdown') {
            expect(metricsTrace($file))->toBe([]);
            $process->signal(SIGTERM);
        } elseif ($reason === 'slow_consumer') {
            $publish = runPublishProcess([
                dirname(__DIR__).'/Support/publish.php', $root.'/publish', config('murmur.secret'),
                json_encode(['topic' => 'news', 'event' => 'update', 'data' => str_repeat('a', 1024)], JSON_THROW_ON_ERROR),
            ]);
            expect($publish->isSuccessful())->toBeTrue();
        }

        waitForMetricsCondition(fn () => count(metricsTrace($file)) >= 2);
        [$open, $close] = metricsTrace($file);
        expect(array_column(metricsTrace($file), 'phase'))->toBe(['opened', 'closed']);
        expect($open->pid)->toBe($close->pid)->not->toBe($pid);
        expect($open->connectionId)->toBe($id)->toBe($close->connectionId);
        expect($open->node)->toBe($node ?: gethostname().":{$port}")->toBe($close->node);
        expect(array_map(base64_decode(...), $open->topics))->toBe($topics)->toBe(array_map(base64_decode(...), $close->topics));
        expect($open->occurredAt)->toBeGreaterThanOrEqual($before)->toBeLessThanOrEqual($close->occurredAt);
        expect($close->occurredAt)->toBeLessThanOrEqual(time());
        expect($close->durationSeconds)->toBeFloat()->toBeGreaterThanOrEqual(0.0);
        expect($close->reason)->toBe($reason);
        expect(array_keys((array) $open))->toBe(['pid', 'phase', 'connectionId', 'node', 'topics', 'occurredAt']);
        expect(array_keys((array) $close))->toBe(['pid', 'phase', 'connectionId', 'node', 'topics', 'occurredAt', 'durationSeconds', 'reason']);

        if ($reason === 'expired') {
            expect($close->occurredAt)->toBeGreaterThanOrEqual($expiry);
        }
        if ($reason === 'shutdown') {
            waitForMetricsCondition(fn () => ! $process->isRunning());
            expect($process->getExitCode())->toBe(0);
        }
    } finally {
        $reader?->close();
        $process->stop(2);
        unlink($file);
    }
})->with(['client', 'expired', 'slow_consumer', 'error', 'shutdown']);

it('takes turns using the same worker after a slow store and discards the oldest records only at capacity', function () {
    $port = ServerHarness::availablePort();
    $file = tempnam(sys_get_temp_dir(), 'murmur-lifecycle-');
    $process = workerServingProcess($port, [
        'MURMUR_LIFECYCLE_EVENTS' => 'true',
        'MURMUR_LIFECYCLE_INTERVAL' => '0.01',
        'MURMUR_LIFECYCLE_BUFFER' => '4',
        'MURMUR_LIFECYCLE_BATCH' => '1',
        'MURMUR_TEST_LIFECYCLE_FILE' => $file,
        'MURMUR_TEST_METRICS_FILE' => $file,
        'MURMUR_TEST_METRICS_MODE' => 'gate',
        'MURMUR_METRICS_TIMEOUT' => '0.2',
        'MURMUR_METRICS_ONLY' => 'murmur_lifecycle_sent_total,murmur_lifecycle_dropped_total',
    ]);

    try {
        $process->start();
        waitForMetricsCondition(fn () => count(metricsTrace($file, 'waiting')) === 1, $process);

        for ($i = 1; $i <= 3; $i++) {
            $reader = SseReader::open("http://127.0.0.1:{$port}/sse?topics=news&connection=".str_repeat((string) $i, 32));
            $reader->start();
            $reader->close();
            waitForMetricsCondition(function () use ($port): bool {
                $response = await((new Browser)->get("http://127.0.0.1:{$port}/health"));

                return json_decode((string) $response->getBody())->connections === 0;
            }, $process);
        }

        waitForMetricsCondition(fn () => str_contains($process->getErrorOutput(), 'metrics recorder timed out'), $process);
        expect(array_column(metricsTrace($file), 'phase'))->toBe(['start', 'waiting']);

        touch($file.'.release');
        waitForMetricsCondition(fn () => count(metricsTrace($file, 'closed')) === 2, $process);
        waitForMetricsCondition(fn () => count(array_filter(metricsTrace($file, 'saved'), fn (object $entry): bool => $entry->samples[1]->value === 4)) >= 1, $process);

        $trace = metricsTrace($file);
        expect(array_unique(array_column($trace, 'pid')))->toHaveCount(1);
        expect(array_column(metricsTrace($file, 'opened'), 'connectionId'))->toBe([str_repeat('2', 32), str_repeat('3', 32)]);
        expect(array_column(metricsTrace($file, 'closed'), 'connectionId'))->toBe([str_repeat('2', 32), str_repeat('3', 32)]);
        $phases = array_column($trace, 'phase');
        expect(array_search('opened', $phases, true))->toBe(3);
        expect(array_search('closed', $phases, true))->toBeGreaterThan(array_search('start', array_slice($phases, 3, null, true), true));
        $snapshots = metricsTrace($file, 'saved');
        expect(array_column(end($snapshots)->samples, 'value', 'name'))->toBe([
            'murmur_lifecycle_dropped_total' => 2,
            'murmur_lifecycle_sent_total' => 4,
        ]);
    } finally {
        $process->stop(2);
        if (is_file($file.'.release')) {
            unlink($file.'.release');
        }
        unlink($file);
    }
});

it('rejects invalid lifecycle settings before binding the port', function (array $settings, string $message) {
    config($settings + ['murmur.lifecycle.enabled' => true]);

    $socket = new SocketServer('127.0.0.1:0');

    try {
        $this->artisan('murmur:serve', ['--port' => parse_url($socket->getAddress(), PHP_URL_PORT)])
            ->expectsOutputToContain($message)->assertExitCode(1);
    } finally {
        $socket->close();
    }
})->with([
    [['murmur.lifecycle.buffer' => 0], 'lifecycle.buffer must be a positive integer'],
    [['murmur.lifecycle.batch' => 2.5], 'lifecycle.batch must be a positive integer'],
    [['murmur.lifecycle.batch' => 1001], 'lifecycle.batch must not exceed lifecycle.buffer'],
    [['murmur.lifecycle.interval' => INF], 'lifecycle.interval must be a positive, finite number'],
    [['murmur.lifecycle.interval' => 0], 'lifecycle.interval must be a positive, finite number'],
    [['murmur.metrics.node' => "invalid\n"], 'valid worker node name'],
]);

it('sends every final batch for connections closed together on shutdown', function () {
    $port = ServerHarness::availablePort();
    $file = tempnam(sys_get_temp_dir(), 'murmur-lifecycle-');
    $process = workerServingProcess($port, [
        'MURMUR_METRICS_STORE' => '',
        'MURMUR_LIFECYCLE_EVENTS' => 'true',
        'MURMUR_LIFECYCLE_INTERVAL' => '60',
        'MURMUR_LIFECYCLE_BATCH' => '2',
        'MURMUR_TEST_LIFECYCLE_FILE' => $file,
        'MURMUR_SHUTDOWN_SPREAD' => '0',
    ]);

    try {
        $process->start();
        waitForMetricsCondition(fn () => str_contains($process->getOutput(), 'Murmur is listening'), $process);

        $ids = array_map(fn (int $i) => str_repeat((string) $i, 32), range(1, 3));
        foreach ($ids as $id) {
            SseReader::open("http://127.0.0.1:{$port}/sse?topics=news&connection={$id}")->start();
        }

        expect(metricsTrace($file))->toBe([]);
        $process->signal(SIGTERM);
        waitForMetricsCondition(fn () => ! $process->isRunning());

        expect($process->getExitCode())->toBe(0);
        expect(array_column(metricsTrace($file, 'opened'), 'connectionId'))->toBe($ids);
        expect(array_column(metricsTrace($file, 'closed'), 'connectionId'))->toBe($ids);
        expect(array_column(metricsTrace($file, 'closed'), 'reason'))->toBe(['shutdown', 'shutdown', 'shutdown']);
    } finally {
        $process->stop(2);
        unlink($file);
    }
})->skip(! extension_loaded('pcntl'), 'Signal-driven shutdown requires ext-pcntl.');

it('keeps serving and sends subsequent batches after a lifecycle listener fails', function () {
    $port = ServerHarness::availablePort();
    $file = tempnam(sys_get_temp_dir(), 'murmur-lifecycle-');
    $process = workerServingProcess($port, [
        'MURMUR_METRICS_STORE' => '',
        'MURMUR_LIFECYCLE_EVENTS' => 'true',
        'MURMUR_LIFECYCLE_INTERVAL' => '0.02',
        'MURMUR_LIFECYCLE_BATCH' => '1',
        'MURMUR_TEST_LIFECYCLE_FILE' => $file,
        'MURMUR_TEST_LIFECYCLE_MODE' => 'fail-first',
    ]);
    $reader = null;

    try {
        $process->start();
        waitForMetricsCondition(fn () => str_contains($process->getOutput(), 'Murmur is listening'), $process);
        $reader = SseReader::open("http://127.0.0.1:{$port}/sse?topics=news");
        $reader->start();
        waitForMetricsCondition(fn () => str_contains($process->getErrorOutput(), 'could not process the lifecycle message'), $process);
        $reader->close();
        waitForMetricsCondition(fn () => count(metricsTrace($file, 'closed')) === 1, $process);

        expect(array_column(metricsTrace($file), 'phase'))->toBe(['opened', 'reported', 'closed']);
        expect(array_unique(array_column(metricsTrace($file), 'pid')))->toHaveCount(1);
        expect(metricsTrace($file, 'closed')[0]->reason)->toBe('client');
    } finally {
        $reader?->close();
        $process->stop(2);
        unlink($file);
    }
});

it('forwards listener output to server stderr without consuming acknowledgement slots', function () {
    $port = ServerHarness::availablePort();
    $file = tempnam(sys_get_temp_dir(), 'murmur-lifecycle-');
    $process = workerServingProcess($port, [
        'MURMUR_LIFECYCLE_EVENTS' => 'true',
        'MURMUR_LIFECYCLE_INTERVAL' => '0.01',
        'MURMUR_LIFECYCLE_BATCH' => '1',
        'MURMUR_TEST_LIFECYCLE_FILE' => $file,
        'MURMUR_TEST_LIFECYCLE_MODE' => 'noisy',
        'MURMUR_TEST_METRICS_FILE' => $file,
        'MURMUR_METRICS_ONLY' => 'murmur_errors_total',
    ]);

    try {
        $process->start();
        waitForMetricsCondition(fn () => count(metricsTrace($file, 'saved')) >= 1, $process);
        $reader = SseReader::open("http://127.0.0.1:{$port}/sse?topics=news");
        $reader->start();
        $reader->close();

        waitForMetricsCondition(fn () => count(metricsTrace($file, 'closed')) === 1, $process);
        $snapshots = count(metricsTrace($file, 'saved'));
        waitForMetricsCondition(fn () => count(metricsTrace($file, 'saved')) > $snapshots, $process);

        expect($process->getErrorOutput())->toContain('listener echo', 'listener dump', 'listener without newline')
            ->not->toContain('acknowledgement');
        expect($process->getOutput())->not->toContain('listener echo', 'listener dump', 'listener without newline');
        expect(array_map(fn ($entry) => $entry->samples[0]->value, metricsTrace($file, 'saved')))
            ->each->toBe(0);
    } finally {
        $process->stop(2);
        unlink($file);
    }
});

it('flushes every shutdown close with the default spread or counts undelivered records at the deadline', function (int $count, string $mode) {
    $port = ServerHarness::availablePort();
    $file = tempnam(sys_get_temp_dir(), 'murmur-lifecycle-');
    $process = workerServingProcess($port, [
        'MURMUR_METRICS_STORE' => '',
        'MURMUR_LIFECYCLE_EVENTS' => 'true',
        'MURMUR_TEST_LIFECYCLE_FILE' => $file,
        'MURMUR_TEST_LIFECYCLE_MODE' => $mode,
    ]);

    try {
        $process->start();
        waitForMetricsCondition(fn () => str_contains($process->getOutput(), 'Murmur is listening'), $process);

        $ids = array_map(fn (int $i) => sprintf('%032x', $i), range(1, $count));
        foreach ($ids as $id) {
            SseReader::open("http://127.0.0.1:{$port}/sse?topics=news&connection={$id}")->start();
        }

        waitForMetricsCondition(fn () => count(metricsTrace($file, 'opened')) === $count, $process);
        $start = hrtime(true);
        $process->signal(SIGTERM);
        waitForMetricsCondition(fn () => ! $process->isRunning());
        $elapsed = (hrtime(true) - $start) / 1_000_000_000;

        expect($process->getExitCode())->toBe(0, $process->getErrorOutput())
            ->and($elapsed)->toBeLessThan(5.4);

        $closed = metricsTrace($file, 'closed');
        if ($mode === 'slow-shutdown') {
            expect($elapsed)->toBeGreaterThanOrEqual(5.0)
                ->and($closed)->toBe([])
                ->and(metricsTrace($file, 'waiting'))->toHaveCount(1)
                ->and(metricsTrace($file, 'shutdown')[0]->dropped)->toBe($count);
        } else {
            expect(array_column($closed, 'connectionId'))->toBe($ids)
                ->and(array_unique(array_column($closed, 'reason')))->toBe(['shutdown']);
        }
    } finally {
        $process->stop(2);
        unlink($file);
    }
})->with([
    '10 healthy streams' => [10, ''],
    '50 healthy streams' => [50, ''],
    '500 healthy streams' => [500, ''],
    '150 streams with a blocked listener' => [150, 'slow-shutdown'],
])->skip(! extension_loaded('pcntl'), 'Signal-driven shutdown requires ext-pcntl.');
