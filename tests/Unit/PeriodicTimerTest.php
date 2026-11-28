<?php

use Arttecs\Murmur\Server\PeriodicTimer;
use React\EventLoop\StreamSelectLoop;

/**
 * Run ten ticks that each block the loop and return the elapsed seconds.
 */
function elapsedForTenBlockingTicks(bool $fixedRate): float
{
    $loop = new StreamSelectLoop;
    $ticks = 0;
    $startedAt = hrtime(true);
    $timer = new PeriodicTimer($loop, 0.02, function () use ($loop, &$ticks): void {
        usleep(15_000);

        if (++$ticks === 10) {
            $loop->stop();
        }
    }, $fixedRate);

    try {
        $loop->run();
    } finally {
        $timer->cancel();
    }

    return (hrtime(true) - $startedAt) / 1_000_000_000;
}

it('keeps fixed-rate deadlines one interval apart when invocations run late', function () {
    $fixed = elapsedForTenBlockingTicks(true);
    $drifting = elapsedForTenBlockingTicks(false);

    expect($fixed)->toBeGreaterThan(0.19)->toBeLessThan(0.28)
        ->and($drifting)->toBeGreaterThan(0.33);
});
