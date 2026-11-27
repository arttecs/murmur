<?php

use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\Server\ErrorReporter;
use Arttecs\Murmur\Server\LifecycleRecorder;
use Arttecs\Murmur\Server\Metrics;
use Arttecs\Murmur\Tests\Support\ReadyTimerLoop;
use Arttecs\Murmur\Worker\LifecycleConfig;
use Arttecs\Murmur\Worker\Worker;
use Psr\Log\NullLogger;
use React\EventLoop\Loop;
use React\Promise\Deferred;

use function React\Async\await;
use function React\Promise\Timer\sleep;

it('ignores ready worker timers cancelled when the child is reaped', function (int $timerIndex) {
    config([
        'murmur.metrics.store' => 'cache',
        'murmur.lifecycle.enabled' => true,
    ]);
    $loop = new ReadyTimerLoop(Loop::get());
    $logger = new NullLogger;
    $errors = 0;
    $countError = function () use (&$errors): void {
        $errors++;
    };
    ErrorReporter::logUsing($logger);
    ErrorReporter::countUsing($countError);

    $lifecycle = LifecycleConfig::from(config());
    $recorder = new LifecycleRecorder($lifecycle->buffer);
    $worker = new Worker($loop, Murmur::metricsConfiguration(), 'test-node', 'true', $lifecycle, $recorder);

    try {
        $worker->launch();
        $process = (new ReflectionProperty(Worker::class, 'process'))->getValue($worker);
        $ack = $process->pipes[Worker::ACK_DESCRIPTOR];
        $ack->pause();

        while ($process->isRunning()) {
            await(sleep(0.001));
        }

        $worker->start(new Metrics);
        $drained = new Deferred;
        $loop->queue($loop->timers()[$timerIndex]);
        $stopping = $worker->stop($drained->promise());
        $ack->close();
        await($stopping);
        expect($worker->running())->toBeFalse();
        $loop->dispatch();

        expect($loop->timers())->toBeEmpty()
            ->and($errors)->toBe(0);
    } finally {
        ErrorReporter::forget($countError, $logger);
    }
})->with([
    'exit monitor' => [0],
    'lifecycle interval' => [1],
    'metrics interval' => [2],
    'acknowledgement deadline' => [3],
]);

it('ignores a ready termination timer after the child is reaped', function () {
    $loop = new ReadyTimerLoop(Loop::get());
    $worker = new Worker($loop, null, 'test-node', 'true');
    $worker->launch();
    $process = (new ReflectionProperty(Worker::class, 'process'))->getValue($worker);
    $ack = $process->pipes[Worker::ACK_DESCRIPTOR];
    $ack->pause();

    while ($process->isRunning()) {
        await(sleep(0.001));
    }

    $stopping = $worker->stop();
    $loop->queue($loop->timers()[1]);
    $ack->close();
    await($stopping);
    $loop->dispatch();

    expect($worker->running())->toBeFalse()
        ->and($loop->timers())->toBeEmpty();
});
