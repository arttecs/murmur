<?php

use Symfony\Component\Process\Process;

function recordMetricsInput(string $input, ?string $store, string $file, string $mode = ''): Process
{
    $process = new Process([PHP_BINARY, dirname(__DIR__).'/Support/metrics-artisan.php', 'murmur:record-metrics'], env: [
        'MURMUR_METRICS_STORE' => $store ?? '',
        'MURMUR_TEST_METRICS_FILE' => $file,
        'MURMUR_TEST_METRICS_MODE' => $mode,
    ], input: $input, timeout: 3);
    $process->run();

    return $process;
}

function metricsRecordPayload(): array
{
    return [
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

it('stores valid stdin without filtering or changing its timestamp', function () {
    $file = tempnam(sys_get_temp_dir(), 'murmur-record-');

    try {
        $payload = metricsRecordPayload();
        $process = recordMetricsInput(json_encode($payload, JSON_THROW_ON_ERROR), 'fake', $file);

        expect($process->getExitCode())->toBe(0, $process->getErrorOutput());
        expect($process->getOutput().$process->getErrorOutput())->toBe('');
        $saved = json_decode(file($file, FILE_IGNORE_NEW_LINES)[1], true, flags: JSON_THROW_ON_ERROR);
        $payload['samples'][1]['labels'] = [];
        expect(array_intersect_key($saved, $payload))->toBe($payload);
    } finally {
        unlink($file);
    }
});

it('rejects invalid stdin on stderr without reaching the store', function (Closure $makeInput) {
    $file = tempnam(sys_get_temp_dir(), 'murmur-record-');

    try {
        $process = recordMetricsInput($makeInput(metricsRecordPayload()), 'fake', $file);

        expect($process->getExitCode())->toBe(1)
            ->and($process->getOutput())->toBe('')
            ->and($process->getErrorOutput())->toMatch('/\A[^\r\n]+\r?\n\z/')
            ->not->toContain('sensitive-payload')
            ->and(file_get_contents($file))->toBe('');
    } finally {
        unlink($file);
    }
})->with([
    'malformed' => [fn () => '{"sensitive-payload"'],
    'envelope type' => [fn () => '[]'],
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
    'non-string label values' => [fn ($p) => json_encode([...$p, 'samples' => [[...$p['samples'][0], 'labels' => ['status' => 202]]]])],
    'extra sample field' => [fn ($p) => json_encode([...$p, 'samples' => [[...$p['samples'][0], 'extra' => 1]]])],
    'string value' => [fn ($p) => json_encode([...$p, 'samples' => [[...$p['samples'][0], 'value' => '7']]])],
    'infinite value' => [fn ($p) => str_replace('"value":7', '"value":1e400', json_encode($p))],
    'oversized' => [fn ($p) => json_encode($p).str_repeat(' ', 1024 * 1024)],
]);

it('reports no store and hides exceptions thrown by a configured store', function (?string $store, string $mode, string $message) {
    $file = tempnam(sys_get_temp_dir(), 'murmur-record-');

    try {
        $process = recordMetricsInput(json_encode(metricsRecordPayload(), JSON_THROW_ON_ERROR), $store, $file, $mode);

        expect($process->getExitCode())->toBe(1)
            ->and($process->getOutput())->toBe('')
            ->and($process->getErrorOutput())->toBe($message."\n")
            ->not->toContain('sensitive store exception');
    } finally {
        unlink($file);
    }
})->with([[null, '', 'No Murmur metrics store is configured.'], ['fake', 'throw', 'Murmur could not write the metrics snapshot.']]);

it('keeps the stdin writer hidden from the Artisan command list', function () {
    $process = new Process([PHP_BINARY, dirname(__DIR__).'/Support/metrics-artisan.php', 'list', '--raw']);
    $process->mustRun();

    expect($process->getOutput())->toContain('murmur:serve')->not->toContain('murmur:record-metrics');
});
