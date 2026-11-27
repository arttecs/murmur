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
use function React\Async\delay;

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

it('rejects malformed envelopes and drops malformed records individually', function (Closure $invalid, bool $validEnvelope = false) {
    $file = tempnam(sys_get_temp_dir(), 'murmur-lifecycle-worker-');

    try {
        $open = lifecycleWorkerRecord();
        $closed = lifecycleWorkerRecord(['occurredAt' => 124, 'durationSeconds' => 1.5, 'reason' => 'expired']);
        $process = lifecycleWorkerProcess($file, $invalid($open, $closed).lifecycleWorkerLine([$open, $closed]), [
            'MURMUR_TEST_LIFECYCLE_MODE' => 'report',
        ]);

        expect($process->getExitCode())->toBe(0, $process->getErrorOutput());
        expect($process->getOutput())->toBe("error\nok\n");
        expect($process->getErrorOutput())->toBe('');
        expect(array_column(metricsTrace($file), 'phase'))->toBe($validEnvelope
            ? ['opened', 'reported', 'opened', 'closed']
            : ['opened', 'closed']);
        expect(array_column(metricsTrace($file, 'reported'), 'message'))->toBe($validEnvelope
            ? ['Murmur lifecycle batch had 1 invalid record.']
            : []);
    } finally {
        unlink($file);
    }
})->with([
    'empty batch' => [fn () => lifecycleWorkerLine([])],
    'batch is an object' => [fn ($open) => lifecycleWorkerLine([], ['records' => (object) [$open]])],
    'too many records' => [fn ($open) => lifecycleWorkerLine([$open, $open, $open])],
    'invalid node' => [fn ($open) => lifecycleWorkerLine([$open], ['node' => "bad\n"])],
    'extra envelope field' => [fn ($open) => lifecycleWorkerLine([$open], ['payload' => 'bad'])],
    'record is not an object' => [fn ($open) => lifecycleWorkerLine([$open, null]), true],
    'unanchored ID' => [fn ($open) => lifecycleWorkerLine([$open, lifecycleWorkerRecord(['connectionId' => str_repeat('a', 32)."\n"])]), true],
    'ID is not a string' => [fn ($open) => lifecycleWorkerLine([$open, lifecycleWorkerRecord(['connectionId' => 123])]), true],
    'empty topics' => [fn ($open) => lifecycleWorkerLine([$open, lifecycleWorkerRecord(['topics' => []])]), true],
    'topics are not a list' => [fn ($open) => lifecycleWorkerLine([$open, lifecycleWorkerRecord(['topics' => (object) $open['topics']])]), true],
    'too many topics' => [fn ($open) => lifecycleWorkerLine([$open, lifecycleWorkerRecord(['topics' => array_map(fn ($i) => base64_encode("topic-{$i}"), range(0, 50))])]), true],
    'invalid wire topic' => [fn ($open) => lifecycleWorkerLine([$open, lifecycleWorkerRecord(['topics' => [base64_encode('not a topic')]])]), true],
    'duplicate topic' => [fn ($open) => lifecycleWorkerLine([$open, lifecycleWorkerRecord(['topics' => [base64_encode('news'), base64_encode('news')]])]), true],
    'invalid topic encoding' => [fn ($open) => lifecycleWorkerLine([$open, lifecycleWorkerRecord(['topics' => ['bmV3cw']])]), true],
    'zero timestamp' => [fn ($open) => lifecycleWorkerLine([$open, lifecycleWorkerRecord(['occurredAt' => 0])]), true],
    'float timestamp' => [fn ($open) => lifecycleWorkerLine([$open, lifecycleWorkerRecord(['occurredAt' => 1.5])]), true],
    'unknown reason' => [fn ($open, $closed) => lifecycleWorkerLine([$open, [...$closed, 'reason' => 'unknown']]), true],
    'missing duration' => [fn ($open, $closed) => lifecycleWorkerLine([$open, array_diff_key($closed, ['durationSeconds' => true])]), true],
    'negative duration' => [fn ($open, $closed) => lifecycleWorkerLine([$open, [...$closed, 'durationSeconds' => -1]]), true],
    'string duration' => [fn ($open, $closed) => lifecycleWorkerLine([$open, [...$closed, 'durationSeconds' => '1.5']]), true],
    'infinite duration' => [fn ($open, $closed) => str_replace('"durationSeconds":1.5', '"durationSeconds":1e400', lifecycleWorkerLine([$open, $closed])), true],
    'extra record field' => [fn ($open) => lifecycleWorkerLine([$open, [...$open, 'token' => 'bad']]), true],
]);

it('reports one invalid record count and dispatches the surrounding records in order', function () {
    $file = tempnam(sys_get_temp_dir(), 'murmur-lifecycle-worker-');

    try {
        $open = lifecycleWorkerRecord();
        $closed = lifecycleWorkerRecord(['occurredAt' => 124, 'durationSeconds' => 1.0, 'reason' => 'client']);
        $process = lifecycleWorkerProcess($file, lifecycleWorkerLine([$open, 'sensitive record', $closed, null]), [
            'MURMUR_LIFECYCLE_BATCH' => '4',
            'MURMUR_TEST_LIFECYCLE_MODE' => 'report',
        ]);

        expect($process->getExitCode())->toBe(0, $process->getErrorOutput())
            ->and($process->getOutput())->toBe("error\n")
            ->and($process->getErrorOutput())->toBe('')
            ->and(array_column(metricsTrace($file), 'phase'))->toBe(['opened', 'closed', 'reported'])
            ->and(array_column(metricsTrace($file, 'reported'), 'message'))->toBe(['Murmur lifecycle batch had 2 invalid records.']);
    } finally {
        unlink($file);
    }
});

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
        'murmur.lifecycle.interval' => $large ? 0.2 : 0.5,
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
        if (! $large) {
            waitForMetricsCondition(fn () => $metrics->value('murmur_lifecycle_sent_total') >= 2);
            delay(0.2);
            expect($metrics->value('murmur_lifecycle_sent_total'))->toBe(4);
        }

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

        expect($metrics->value('murmur_lifecycle_dropped_total'))->toBe(2);
        $hub->attach($second, ['news']);
        $second->close();
        expect($metrics->value('murmur_lifecycle_dropped_total'))->toBe(4);
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

it('drops and reports an unencodable lifecycle record while retaining the rest of the batch', function () {
    $errors = 0;
    $counter = function () use (&$errors): void {
        $errors++;
    };
    $logger = new NullLogger;
    ErrorReporter::countUsing($counter);
    ErrorReporter::logUsing($logger);
    $recorder = new LifecycleRecorder(3);
    $record = [
        'connectionId' => str_repeat('a', 32),
        'topics' => ['news'],
        'occurredAt' => 123,
    ];
    $recorder->enqueue($record);
    $recorder->enqueue($record + ['reason' => 'error', 'durationSeconds' => INF]);
    $recorder->enqueue(['occurredAt' => 124] + $record);

    try {
        [$payload, $sent, $dropped] = $recorder->batch('edge', 3, Worker::MAX_LINE_BYTES);

        expect($sent)->toBe(2)
            ->and($dropped)->toBe(1)
            ->and($errors)->toBe(1)
            ->and(array_column(json_decode($payload, true, flags: JSON_THROW_ON_ERROR)['records'], 'occurredAt'))->toBe([123, 124])
            ->and($recorder->pending())->toBeFalse();
    } finally {
        ErrorReporter::forget($counter, $logger);
    }
});

it('clears scoped instances and both forms of logging context between lifecycle messages', function () {
    $file = tempnam(sys_get_temp_dir(), 'murmur-lifecycle-worker-');

    try {
        $line = lifecycleWorkerLine([lifecycleWorkerRecord()]);
        $process = lifecycleWorkerProcess($file, $line.$line, ['MURMUR_TEST_LIFECYCLE_MODE' => 'hygiene']);

        expect($process->getExitCode())->toBe(0, $process->getErrorOutput())
            ->and($process->getOutput())->toBe("ok\nok\n")
            ->and(array_map(fn (object $entry): array => (array) $entry->state, metricsTrace($file, 'hygiene')))->toBe([
                ['uses' => 0, 'shared' => [], 'channel' => [], 'context' => [], 'hidden' => []],
                ['uses' => 0, 'shared' => [], 'channel' => [], 'context' => [], 'hidden' => []],
            ]);
    } finally {
        unlink($file);
    }
});
