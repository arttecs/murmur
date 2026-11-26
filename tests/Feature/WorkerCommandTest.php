<?php

use Symfony\Component\Process\Process;

function recordMetricsInput(string|iterable $input, ?string $store, string $file, string $mode = ''): Process
{
    $command = [PHP_BINARY, '-d', 'memory_limit=32M', dirname(__DIR__).'/Support/metrics-artisan.php', 'murmur:worker'];
    $process = Process::fromShellCommandLine('exec '.implode(' ', array_map(escapeshellarg(...), $command)).' 3>&1 1>&2', env: [
        'MURMUR_METRICS_STORE' => $store ?? '',
        'MURMUR_TEST_METRICS_FILE' => $file,
        'MURMUR_TEST_METRICS_MODE' => $mode,
    ], input: $input, timeout: 8);
    $process->run();

    return $process;
}

function metricsRecordPayload(): array
{
    return [
        'type' => 'metrics',
        'node' => 'edge.test:8080',
        'recorded_at' => 123,
        'samples' => [[
            'name' => 'murmur_publish_requests_total',
            'type' => 'counter',
            'help' => 'A response was sent.',
            'labels' => ['status' => '202'],
            'value' => 7,
        ], [
            'name' => 'murmur_uptime_seconds',
            'type' => 'gauge',
            'help' => 'Elapsed time.',
            'labels' => (object) [],
            'value' => 0.5,
        ]],
    ];
}

it('stores successive lines without filtering or changing their timestamps and exits on EOF', function () {
    $file = tempnam(sys_get_temp_dir(), 'murmur-record-');

    try {
        $payload = metricsRecordPayload();
        $next = [...$payload, 'recorded_at' => 124];
        $process = recordMetricsInput(json_encode($payload, JSON_THROW_ON_ERROR)."\n".json_encode($next, JSON_THROW_ON_ERROR)."\n", 'fake', $file);

        expect($process->getExitCode())->toBe(0, $process->getErrorOutput());
        expect($process->getOutput())->toBe("ok\nok\n");
        expect($process->getErrorOutput())->toBe('');
        [$first, $second] = metricsTrace($file, 'saved');
        expect($first->pid)->toBe($second->pid);
        $payload['samples'][1]['labels'] = [];
        $saved = json_decode(json_encode($first, JSON_THROW_ON_ERROR), true, flags: JSON_THROW_ON_ERROR);
        unset($payload['type']);
        expect(array_intersect_key($saved, $payload))->toBe($payload);
        expect($second->recorded_at)->toBe(124);
    } finally {
        unlink($file);
    }
});

it('rejects invalid lines without reaching the store and handles the next valid line', function (Closure $makeInput) {
    $file = tempnam(sys_get_temp_dir(), 'murmur-record-');

    try {
        $payload = metricsRecordPayload();
        $process = recordMetricsInput($makeInput($payload)."\n".json_encode($payload, JSON_THROW_ON_ERROR)."\n", 'fake', $file);

        expect($process->getExitCode())->toBe(0)
            ->and($process->getOutput())->toBe("error\nok\n")
            ->and($process->getErrorOutput())->toBe('')
            ->and(metricsTrace($file, 'start'))->toHaveCount(1);
    } finally {
        unlink($file);
    }
})->with([
    'malformed' => [fn () => '{"sensitive-payload"'],
    'envelope type' => [fn () => '[]'],
    'missing message type' => [fn ($p) => json_encode(array_diff_key($p, ['type' => true]))],
    'unknown message type' => [fn ($p) => json_encode([...$p, 'type' => 'unknown'])],
    'extra field' => [fn ($p) => json_encode($p + ['extra' => 'sensitive-payload'])],
    'missing node' => [fn ($p) => json_encode(array_diff_key($p, ['node' => true]))],
    'invalid node' => [fn ($p) => json_encode(array_replace($p, ['node' => "sensitive-payload\n"]))],
    'node type' => [fn ($p) => json_encode(array_replace($p, ['node' => 123]))],
    'zero timestamp' => [fn ($p) => json_encode(array_replace($p, ['recorded_at' => 0]))],
    'string timestamp' => [fn ($p) => json_encode(array_replace($p, ['recorded_at' => '1']))],
    'float timestamp' => [fn ($p) => json_encode(array_replace($p, ['recorded_at' => 1.5]))],
    'samples object' => [fn ($p) => json_encode(array_replace($p, ['samples' => (object) $p['samples']]))],
    'sample not an object' => [fn ($p) => json_encode(array_replace($p, ['samples' => [null]]))],
    'unknown name' => [fn ($p) => json_encode([...$p, 'samples' => [[...$p['samples'][0], 'name' => 'sensitive-payload']]])],
    'non-string name' => [fn ($p) => json_encode([...$p, 'samples' => [[...$p['samples'][0], 'name' => 1]]])],
    'invalid type' => [fn ($p) => json_encode([...$p, 'samples' => [[...$p['samples'][0], 'type' => 'histogram']]])],
    'non-string help' => [fn ($p) => json_encode([...$p, 'samples' => [[...$p['samples'][0], 'help' => null]]])],
    'array labels' => [fn ($p) => json_encode([...$p, 'samples' => [[...$p['samples'][0], 'labels' => []]]])],
    'invalid label name' => [fn ($p) => json_encode([...$p, 'samples' => [[...$p['samples'][0], 'labels' => ["invalid\n" => '202']]]])],
    'non-string label values' => [fn ($p) => json_encode([...$p, 'samples' => [[...$p['samples'][0], 'labels' => ['status' => 202]]]])],
    'extra sample field' => [fn ($p) => json_encode([...$p, 'samples' => [[...$p['samples'][0], 'extra' => 1]]])],
    'string value' => [fn ($p) => json_encode([...$p, 'samples' => [[...$p['samples'][0], 'value' => '7']]])],
    'infinite value' => [fn ($p) => str_replace('"value":7', '"value":1e400', json_encode($p))],
    'oversized' => [fn ($p) => json_encode($p).str_repeat(' ', 1024 * 1024)],
]);

it('discards an over-long line with bounded memory and accepts a line at the exact limit', function () {
    $file = tempnam(sys_get_temp_dir(), 'murmur-record-');

    try {
        $payload = json_encode(metricsRecordPayload(), JSON_THROW_ON_ERROR);
        $input = (function () use ($payload): Generator {
            for ($i = 0; $i < 640; $i++) {
                yield str_repeat(' ', 65536);
            }

            yield "\n".$payload.str_repeat(' ', 1_048_576 - strlen($payload))."\n";
        })();
        $process = recordMetricsInput($input, 'fake', $file);

        expect($process->getExitCode())->toBe(0, $process->getErrorOutput());
        expect($process->getOutput())->toBe("error\nok\n");
        expect(metricsTrace($file, 'saved'))->toHaveCount(1);
    } finally {
        unlink($file);
    }
});

it('refuses invalid store configuration at startup before reading stdin', function (?string $store, string $message) {
    $process = recordMetricsInput('', $store, '');

    expect($process->getExitCode())->toBe(1)
        ->and($process->getOutput())->toBe('')
        ->and($process->getErrorOutput())->toBe($message."\n");
})->with([
    [null, 'Murmur worker requires a metrics store or lifecycle events.'],
    ['unknown', 'Murmur metrics store configuration is invalid.'],
]);

it('acknowledges a failed write and continues even when the exception handler throws', function () {
    $file = tempnam(sys_get_temp_dir(), 'murmur-record-');

    try {
        $line = json_encode(metricsRecordPayload(), JSON_THROW_ON_ERROR)."\n";
        $process = recordMetricsInput($line.$line, 'fake', $file, 'fail-first');
        $events = metricsTrace($file);

        expect($process->getExitCode())->toBe(0);
        expect($process->getOutput())->toBe("error\nok\n");
        expect($process->getErrorOutput())->toBe('');
        expect(array_column($events, 'phase'))->toBe(['start', 'reported', 'start', 'saved']);
        expect($events[1]->message)->toBe('sensitive store exception');
        expect(array_unique(array_column($events, 'pid')))->toHaveCount(1);
    } finally {
        unlink($file);
    }
});

it('keeps the database query log empty across writes', function () {
    $file = tempnam(sys_get_temp_dir(), 'murmur-record-');

    try {
        $line = json_encode(metricsRecordPayload(), JSON_THROW_ON_ERROR)."\n";
        $process = recordMetricsInput($line.$line, 'fake', $file, 'query-log');

        expect($process->getExitCode())->toBe(0);
        expect($process->getOutput())->toBe("ok\nok\n");
        expect(array_column(metricsTrace($file, 'queries'), 'count'))->toBe([0, 0]);
    } finally {
        unlink($file);
    }
});

it('keeps the stdin writer hidden from the Artisan command list', function () {
    $process = new Process([PHP_BINARY, dirname(__DIR__).'/Support/metrics-artisan.php', 'list', '--raw']);
    $process->mustRun();

    expect($process->getOutput())->toContain('murmur:serve')->not->toContain('murmur:worker');
});

it('keeps writing when its parent is the init process', function () {
    $file = tempnam(sys_get_temp_dir(), 'murmur-record-');
    $line = json_encode(metricsRecordPayload(), JSON_THROW_ON_ERROR);
    $writer = implode(' ', array_map(escapeshellarg(...), [PHP_BINARY, dirname(__DIR__).'/Support/metrics-artisan.php', 'murmur:worker']));
    $feed = 'printf "%s\n" "$LINE"; sleep 2; printf "%s\n" "$LINE"';
    $process = Process::fromShellCommandLine("( ( {$feed} ) | {$writer} 3>&1 > /dev/null 2>&1 | cat > /dev/null & )", env: [
        'LINE' => $line,
        'MURMUR_METRICS_STORE' => 'fake',
        'MURMUR_TEST_METRICS_FILE' => $file,
    ], timeout: 5);

    try {
        $process->mustRun();
        waitForMetricsCondition(fn () => count(metricsTrace($file, 'saved')) >= 2);
        [$first, $second] = metricsTrace($file, 'saved');

        expect($second->pid)->toBe($first->pid);
    } finally {
        unlink($file);
    }
})->skip(! extension_loaded('pcntl') || ! extension_loaded('posix'), 'The parent check requires ext-pcntl and ext-posix.');

it('refuses to run without a dedicated acknowledgement pipe', function () {
    $command = [PHP_BINARY, dirname(__DIR__).'/Support/metrics-artisan.php', 'murmur:worker'];
    $process = Process::fromShellCommandLine('exec '.implode(' ', array_map(escapeshellarg(...), $command)).' 3>&-', env: [
        'MURMUR_LIFECYCLE_EVENTS' => 'true',
    ]);
    $process->run();

    expect($process->getExitCode())->toBe(1)
        ->and($process->getOutput())->toBe('')
        ->and($process->getErrorOutput())->toBe("Murmur worker requires an acknowledgement pipe on descriptor 3.\n");
});
