<?php

use Arttecs\Murmur\Console\Commands\CollectCycles;
use Arttecs\Murmur\Server\Metrics;
use Arttecs\Murmur\Server\PeriodicTimer;
use React\EventLoop\StreamSelectLoop;

it('adds the result of each scheduled cycle collection to the counter', function () {
    $loop = new StreamSelectLoop;
    $metrics = new Metrics;
    $collect = new CollectCycles($metrics);
    $results = [3, 0, 2];
    $timer = new PeriodicTimer($loop, 0.001, function () use ($collect, $loop, &$results): void {
        $collect(function () use (&$results): int {
            return array_shift($results);
        });

        if ($results === []) {
            $loop->stop();
        }
    });

    try {
        $loop->run();
    } finally {
        $timer->cancel();
    }

    expect(Metrics::names())->toContain('murmur_gc_collected_cycles_total')
        ->and($metrics->value('murmur_gc_collected_cycles_total'))->toBe(5)
        ->and($metrics->toPrometheus())->toContain('# TYPE murmur_gc_collected_cycles_total counter', 'murmur_gc_collected_cycles_total 5');
});
