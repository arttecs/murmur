<?php

use Arttecs\Murmur\Server\ErrorReporter;
use Arttecs\Murmur\Server\Hub;
use Arttecs\Murmur\Server\LifecycleRecorder;
use Arttecs\Murmur\Server\Metrics;
use Arttecs\Murmur\Worker\LifecycleConfig;
use Arttecs\Murmur\Worker\Worker;
use Psr\Log\NullLogger;
use React\EventLoop\Loop;
use Symfony\Component\Process\Process;

use function React\Async\await;

function lifecycleWorkerRecord(array $fields = []): array
{
    return $fields + [
        'connectionId' => str_repeat('a', 32),
        'topics' => [base64_encode('news'), base64_encode('private-news')],
        'occurredAt' => 123,
    ];
}

function lifecycleWorkerLine(array $records, array $fields = []): string
{
    return json_encode($fields + ['type' => 'lifecycle', 'node' => 'edge.test:8080', 'records' => $records], JSON_THROW_ON_ERROR)."\n";
}

function lifecycleWorkerProcess(string $file, string|iterable $input, array $env = []): Process
{
    $command = [PHP_BINARY, '-d', 'memory_limit=32M', dirname(__DIR__).'/Support/metrics-artisan.php', 'murmur:worker'];
    $process = Process::fromShellCommandLine('exec '.implode(' ', array_map(escapeshellarg(...), $command)).' 3>&1 1>&2', env: $env + [
        'MURMUR_METRICS_STORE' => '',
        'MURMUR_LIFECYCLE_EVENTS' => 'true',
        'MURMUR_LIFECYCLE_BATCH' => '2',
        'MURMUR_TEST_LIFECYCLE_FILE' => $file,
    ], input: $input, timeout: 8);
    $process->run();

    return $process;
}

it('rejects a malformed lifecycle batch in its entirety and dispatches the next valid batch', function (Closure $invalid) {
    $file = tempnam(sys_get_temp_dir(), 'murmur-lifecycle-worker-');

    try {
        $open = lifecycleWorkerRecord();
        $closed = lifecycleWorkerRecord(['occurredAt' => 124, 'durationSeconds' => 1.5, 'reason' => 'expired']);
        $process = lifecycleWorkerProcess($file, $invalid($open, $closed).lifecycleWorkerLine([$open, $closed]));

        expect($process->getExitCode())->toBe(0, $process->getErrorOutput());
        expect($process->getOutput())->toBe("error\nok\n");
        expect($process->getErrorOutput())->toBe('');
        expect(array_column(metricsTrace($file), 'phase'))->toBe(['opened', 'closed']);
    } finally {
        unlink($file);
    }
})->with([
    'empty batch' => [fn () => lifecycleWorkerLine([])],
    'batch is an object' => [fn ($open) => lifecycleWorkerLine([], ['records' => (object) [$open]])],
    'too many records' => [fn ($open) => lifecycleWorkerLine([$open, $open, $open])],
    'invalid node' => [fn ($open) => lifecycleWorkerLine([$open], ['node' => "bad\n"])],
    'extra envelope field' => [fn ($open) => lifecycleWorkerLine([$open], ['payload' => 'bad'])],
    'record is not an object' => [fn ($open) => lifecycleWorkerLine([$open, null])],
    'unanchored ID' => [fn ($open) => lifecycleWorkerLine([$open, lifecycleWorkerRecord(['connectionId' => str_repeat('a', 32)."\n"])])],
    'ID is not a string' => [fn ($open) => lifecycleWorkerLine([$open, lifecycleWorkerRecord(['connectionId' => 123])])],
    'empty topics' => [fn ($open) => lifecycleWorkerLine([$open, lifecycleWorkerRecord(['topics' => []])])],
    'topics are not a list' => [fn ($open) => lifecycleWorkerLine([$open, lifecycleWorkerRecord(['topics' => (object) $open['topics']])])],
    'too many topics' => [fn ($open) => lifecycleWorkerLine([$open, lifecycleWorkerRecord(['topics' => array_map(fn ($i) => base64_encode("topic-{$i}"), range(0, 50))])])],
    'invalid wire topic' => [fn ($open) => lifecycleWorkerLine([$open, lifecycleWorkerRecord(['topics' => [base64_encode('not a topic')]])])],
    'duplicate topic' => [fn ($open) => lifecycleWorkerLine([$open, lifecycleWorkerRecord(['topics' => [base64_encode('news'), base64_encode('news')]])])],
    'invalid topic encoding' => [fn ($open) => lifecycleWorkerLine([$open, lifecycleWorkerRecord(['topics' => ['bmV3cw']])])],
    'zero timestamp' => [fn ($open) => lifecycleWorkerLine([$open, lifecycleWorkerRecord(['occurredAt' => 0])])],
    'float timestamp' => [fn ($open) => lifecycleWorkerLine([$open, lifecycleWorkerRecord(['occurredAt' => 1.5])])],
    'unknown reason' => [fn ($open, $closed) => lifecycleWorkerLine([$open, [...$closed, 'reason' => 'unknown']])],
    'missing duration' => [fn ($open, $closed) => lifecycleWorkerLine([$open, array_diff_key($closed, ['durationSeconds' => true])])],
    'negative duration' => [fn ($open, $closed) => lifecycleWorkerLine([$open, [...$closed, 'durationSeconds' => -1]])],
    'string duration' => [fn ($open, $closed) => lifecycleWorkerLine([$open, [...$closed, 'durationSeconds' => '1.5']])],
    'infinite duration' => [fn ($open, $closed) => str_replace('"durationSeconds":1.5', '"durationSeconds":1e400', lifecycleWorkerLine([$open, $closed]))],
    'extra record field' => [fn ($open) => lifecycleWorkerLine([$open, [...$open, 'token' => 'bad']])],
]);

it('isolates listener failures within a batch and reports one exception and one summary', function () {
    $file = tempnam(sys_get_temp_dir(), 'murmur-lifecycle-worker-');

    try {
        $records = array_map(fn (int $i) => lifecycleWorkerRecord([
            'occurredAt' => $i,
        ] + ($i % 2 === 1 ? ['durationSeconds' => 0.0, 'reason' => 'shutdown'] : [])), range(1, 7));
        $process = lifecycleWorkerProcess($file, lifecycleWorkerLine(array_slice($records, 0, 5)).lifecycleWorkerLine([$records[6]]), [
            'MURMUR_LIFECYCLE_BATCH' => '5',
            'MURMUR_TEST_LIFECYCLE_MODE' => 'fail-first',
        ]);

        expect($process->getExitCode())->toBe(0, $process->getErrorOutput());
        expect($process->getOutput())->toBe("error\nok\n");
        expect($process->getErrorOutput())->toBe("Murmur lifecycle batch had 2 failed events.\n");
        expect(array_column(metricsTrace($file, 'closed'), 'occurredAt'))->toBe([1, 3, 5, 7]);
        expect(array_column(metricsTrace($file, 'opened'), 'occurredAt'))->toBe([2, 4]);
        expect(array_column(metricsTrace($file, 'reported'), 'message'))->toBe(['The lifecycle listener failed.']);
    } finally {
        unlink($file);
    }
});

it('flushes on its interval and caps ordered batches by count and by encoded size', function (bool $large, array $counts) {
    $file = tempnam(sys_get_temp_dir(), 'murmur-lifecycle-batch-');
    $loop = Loop::get();
    $recorder = new LifecycleRecorder(400);
    config([
        'murmur.lifecycle.enabled' => true,
        'murmur.lifecycle.buffer' => 400,
        'murmur.lifecycle.batch' => $large ? 400 : 2,
        'murmur.lifecycle.interval' => 0.2,
    ]);
    $command = escapeshellarg(PHP_BINARY).' -r '.escapeshellarg('$ack = fopen("php://fd/3", "w"); while ($line = fgets(STDIN)) { file_put_contents($argv[1], $line, FILE_APPEND | LOCK_EX); fwrite($ack, "ok\n"); }').' '.escapeshellarg($file);
    $worker = new Worker($loop, null, 'edge', $command, LifecycleConfig::from(config()), $recorder);
    $metrics = new Metrics;
    $total = $large ? 260 : 5;
    $topics = $large ? array_map(fn ($i) => str_pad((string) $i, 200, 'a'), range(1, 50)) : ['news'];

    try {
        $worker->launch();
        $worker->start($metrics);

        for ($i = 0; $i < $total; $i++) {
            $recorder->enqueue(['connectionId' => sprintf('%032x', $i), 'topics' => $topics, 'occurredAt' => 123]);
        }

        expect($metrics->value('murmur_lifecycle_sent_total'))->toBe(0);
        waitForMetricsCondition(fn () => $metrics->value('murmur_lifecycle_sent_total') === $total);
        await($worker->stop());

        $lines = file($file, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
        $records = array_map(fn (string $line) => json_decode($line, true, flags: JSON_THROW_ON_ERROR)['records'], $lines);
        expect(array_map(count(...), $records))->toBe($counts);
        expect(max(array_map(strlen(...), $lines)))->toBeLessThanOrEqual(1_048_576);
        expect(array_column(array_merge(...$records), 'connectionId'))->toBe(array_map(fn ($i) => sprintf('%032x', $i), range(0, $total - 1)));
    } finally {
        await($worker->stop());
        unlink($file);
    }
})->with(['count' => [false, [2, 2, 1]], 'size' => [true, [76, 76, 76, 32]]]);

it('discards buffered and future lifecycle records when the worker exits', function () {
    $file = tempnam(sys_get_temp_dir(), 'murmur-lifecycle-death-');
    $loop = Loop::get();
    $metrics = new Metrics;
    $recorder = new LifecycleRecorder(10);
    $hub = new Hub($metrics, $recorder);
    $logger = new NullLogger;
    config([
        'murmur.lifecycle.enabled' => true,
        'murmur.lifecycle.buffer' => 10,
        'murmur.lifecycle.batch' => 1,
        'murmur.lifecycle.interval' => 0.02,
    ]);
    $command = escapeshellarg(PHP_BINARY).' -r '.escapeshellarg('fgets(STDIN); file_put_contents($argv[1], (string) getmypid()); sleep(30);').' '.escapeshellarg($file);
    $worker = new Worker($loop, null, 'edge', $command, LifecycleConfig::from(config()), $recorder);
    ErrorReporter::logUsing($logger);
    [$first] = fakeConnection();
    [$second] = fakeConnection();

    try {
        $worker->launch();
        $worker->start($metrics);
        $hub->attach($first, ['news']);
        $first->close();
        waitForMetricsCondition(fn () => (int) file_get_contents($file) > 0);

        expect($metrics->value('murmur_lifecycle_sent_total'))->toBe(1);
        expect($metrics->value('murmur_lifecycle_dropped_total'))->toBe(0);
        expect(posix_kill((int) file_get_contents($file), SIGKILL))->toBeTrue();
        waitForMetricsCondition(fn () => ! $worker->running());

        expect($metrics->value('murmur_lifecycle_dropped_total'))->toBe(1);
        $hub->attach($second, ['news']);
        $second->close();
        expect($metrics->value('murmur_lifecycle_dropped_total'))->toBe(3);
        expect($metrics->value('murmur_lifecycle_sent_total'))->toBe(1);
    } finally {
        await($worker->stop());
        ErrorReporter::forget(null, $logger);
        unlink($file);
    }
})->skip(! extension_loaded('posix'), 'Checking worker death requires ext-posix.');

it('validates decoded lifecycle topic UTF-8 and byte length', function (string $topic, bool $valid) {
    $file = tempnam(sys_get_temp_dir(), 'murmur-lifecycle-worker-');

    try {
        $record = lifecycleWorkerRecord(['topics' => [base64_encode($topic)]]);
        $process = lifecycleWorkerProcess($file, lifecycleWorkerLine([$record]));

        expect($process->getExitCode())->toBe(0, $process->getErrorOutput())
            ->and($process->getOutput())->toBe($valid ? "ok\n" : "error\n")
            ->and(array_column(metricsTrace($file, 'opened'), 'topics'))->toBe($valid ? [$record['topics']] : []);
    } finally {
        unlink($file);
    }
})->with('wire topic boundaries');
