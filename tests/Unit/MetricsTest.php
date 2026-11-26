<?php

use Arttecs\Murmur\Server\Metrics;

it('declares zero value counters in order and reads gauges on each snapshot', function () {
    $metrics = new Metrics;
    $open = 2;
    $metrics->gauge('murmur_connections', 'Open SSE connections.', function () use (&$open): int {
        return $open;
    });

    $snapshot = $metrics->snapshot();
    expect(array_column($snapshot, 'name'))->toBe([
        'murmur_connections_opened_total',
        'murmur_connections_closed_total',
        'murmur_slow_consumers_total',
        'murmur_events_received_total',
        'murmur_events_sent_total',
        'murmur_bytes_sent_total',
        'murmur_replays_total',
        'murmur_replayed_events_total',
        'murmur_gaps_total',
        'murmur_event_loop_lag_exceeded_total',
        'murmur_errors_total',
        'murmur_connections',
    ]);
    expect(array_column($snapshot, 'value'))->toBe([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]);
    expect($snapshot[0])->toBe([
        'name' => 'murmur_connections_opened_total',
        'type' => 'counter',
        'help' => 'SSE connections accepted.',
        'labels' => [],
        'value' => 0,
    ]);

    $open = 4;
    $metrics->increment('murmur_connections_opened_total');
    $snapshot = $metrics->snapshot();

    expect($snapshot[0]['value'])->toBe(1);
    expect(end($snapshot))->toBe([
        'name' => 'murmur_connections',
        'type' => 'gauge',
        'help' => 'Open SSE connections.',
        'labels' => [],
        'value' => 4,
    ]);
});

it('accumulates label sets separately and returns them in deterministic order', function () {
    $metrics = new Metrics;
    $metrics->increment('murmur_publish_requests_total', 2, ['status' => '401']);
    $metrics->increment('murmur_publish_requests_total', labels: ['status' => '202']);
    $metrics->increment('murmur_publish_requests_total', 3, ['status' => '401']);

    $series = array_values(array_filter($metrics->snapshot(), fn (array $sample): bool => $sample['name'] === 'murmur_publish_requests_total'));

    expect(array_column($series, 'labels'))->toBe([['status' => '202'], ['status' => '401']]);
    expect(array_column($series, 'value'))->toBe([1, 5]);
});

it('rejects increments to an undeclared counter', function () {
    expect(fn () => (new Metrics)->increment('murmur_unknown_total'))->toThrow(InvalidArgumentException::class);
});

it('reads the value of a single series', function () {
    $metrics = new Metrics;
    $metrics->gauge('murmur_connections', 'Open SSE connections.', fn (): int => 4);
    $metrics->increment('murmur_events_sent_total', 3);
    $metrics->increment('murmur_publish_requests_total', 2, ['status' => '202']);

    expect($metrics->value('murmur_events_sent_total'))->toBe(3);
    expect($metrics->value('murmur_connections'))->toBe(4);
    expect($metrics->value('murmur_publish_requests_total', ['status' => '202']))->toBe(2);
    expect($metrics->value('murmur_publish_requests_total', ['status' => '401']))->toBeNull();
    expect($metrics->value('murmur_publish_requests_total'))->toBeNull();
    expect($metrics->value('murmur_unknown'))->toBeNull();
});

it('formats a snapshot as prometheus text with escaped label values', function () {
    $metrics = new Metrics;
    $metrics->gauge('murmur_temperature', "Test\\ gauge.\nSecond line.", fn (): float => 3.5);
    $metrics->increment('murmur_publish_requests_total', 2, ['status' => '202']);
    $metrics->increment('murmur_publish_requests_total', 3, ['status' => "line\nwith \"quotes\" and a \\ slash"]);
    $text = $metrics->toPrometheus();

    expect($text)->toStartWith("# HELP murmur_connections_opened_total SSE connections accepted.\n# TYPE murmur_connections_opened_total counter\nmurmur_connections_opened_total 0\n");
    expect($text)->toContain(implode("\n", [
        '# HELP murmur_publish_requests_total HTTP publish responses by status.',
        '# TYPE murmur_publish_requests_total counter',
        'murmur_publish_requests_total{status="line\\nwith \\"quotes\\" and a \\\\ slash"} 3',
        'murmur_publish_requests_total{status="202"} 2',
        '',
    ]));
    expect($text)->toEndWith("# HELP murmur_temperature Test\\\\ gauge.\\nSecond line.\n# TYPE murmur_temperature gauge\nmurmur_temperature 3.5\n");
    expect(substr_count($text, '# TYPE murmur_publish_requests_total'))->toBe(1);
});
