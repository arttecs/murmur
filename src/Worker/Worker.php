<?php

namespace Arttecs\Murmur\Worker;

use Arttecs\Murmur\Metrics\MetricsConfig;
use Arttecs\Murmur\Metrics\Snapshot;
use Arttecs\Murmur\Server\ErrorReporter;
use Arttecs\Murmur\Server\LifecycleRecorder;
use Arttecs\Murmur\Server\Metrics;
use Arttecs\Murmur\Server\PeriodicTimer;
use InvalidArgumentException;
use React\ChildProcess\Process;
use React\EventLoop\LoopInterface;
use React\EventLoop\TimerInterface;
use React\Promise\Deferred;
use React\Promise\PromiseInterface;
use RuntimeException;
use Throwable;

/**
 * Send server records to a single Artisan worker without waiting in the event loop.
 *
 * @internal
 */
class Worker
{
    /**
     * The maximum length of a worker message before its newline.
     */
    public const MAX_LINE_BYTES = 1_048_576;

    /**
     * The descriptor reserved for worker acknowledgements.
     */
    public const ACK_DESCRIPTOR = 3;

    /**
     * The Unix signal that forces a worker to exit.
     */
    protected const KILL_SIGNAL = 9;

    /**
     * The maximum length of a worker acknowledgement before its newline.
     */
    protected const ACK_BYTES = 5;

    /**
     * The timer that takes periodic snapshots.
     */
    protected ?PeriodicTimer $interval = null;

    /**
     * The timer that marks lifecycle batches due.
     */
    protected ?PeriodicTimer $lifecycleInterval = null;

    /**
     * The metrics registry used to collect snapshots and count lifecycle records.
     */
    protected ?Metrics $metrics = null;

    /**
     * Whether a metrics tick has made a snapshot due.
     */
    protected bool $metricsDue = false;

    /**
     * Whether a lifecycle tick has made a batch due.
     */
    protected bool $lifecycleDue = false;

    /**
     * Whether a full batch already has a flush scheduled for the next tick.
     */
    protected bool $flushScheduled = false;

    /**
     * The kind of message last handed to the worker.
     */
    protected ?string $lastSent = null;

    /**
     * Whether the server has finished draining and every waiting record is due.
     */
    protected bool $drained = false;

    /**
     * The number of lifecycle records awaiting acknowledgement.
     */
    protected int $pendingLifecycle = 0;

    /**
     * Whether the worker's input has been closed for shutdown.
     */
    protected bool $inputClosing = false;

    /**
     * The timer that reports a late acknowledgement.
     */
    protected ?TimerInterface $deadline = null;

    /**
     * The timer that advances shutdown from EOF to termination or a forced exit.
     */
    protected ?TimerInterface $shutdownTimer = null;

    /**
     * The timer that detects process exit independently of pipe EOF.
     */
    protected ?PeriodicTimer $exitMonitor = null;

    /**
     * The worker started before the server opened any sockets.
     */
    protected ?Process $process = null;

    /**
     * The message kind that still needs an acknowledgement.
     */
    protected ?string $pending = null;

    /**
     * Whether the worker's input stream cannot accept more data.
     */
    protected bool $backpressured = false;

    /**
     * The partial acknowledgement, or null while discarding an over-long line.
     */
    protected ?string $reply = '';

    /**
     * The pending shutdown, resolved after the worker has been reaped.
     *
     * @var Deferred<null>|null
     */
    protected ?Deferred $stopping = null;

    /**
     * Prepare the worker and verify that this host can launch and signal the worker.
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        protected LoopInterface $loop,
        protected ?MetricsConfig $config,
        protected string $node,
        protected string $command,
        protected ?LifecycleConfig $lifecycle = null,
        protected ?LifecycleRecorder $recorder = null,
    ) {
        if (! Snapshot::validNode($node)) {
            throw new InvalidArgumentException(Snapshot::INVALID_NODE_MESSAGE);
        }

        if (PHP_OS_FAMILY === 'Windows' || ! function_exists('proc_open')) {
            throw new InvalidArgumentException('Murmur worker requires proc_open() on Linux or macOS.');
        }

        if (Process::isSigchildEnabled()) {
            throw new InvalidArgumentException('Murmur worker requires PHP built without --enable-sigchild.');
        }
    }

    /**
     * Launch the worker before the server establishes any network connections.
     *
     * The worker shares the server's standard error so its logs reach the same place.
     *
     * @throws RuntimeException
     */
    public function launch(): void
    {
        $process = new Process('exec '.$this->command, null, null, [['pipe', 'r'], STDERR, STDERR, static::ACK_DESCRIPTOR => ['pipe', 'w']]);

        try {
            $process->start($this->loop, 0.01);
        } catch (RuntimeException $error) {
            throw new RuntimeException('Murmur could not start the worker.', previous: $error);
        }

        $this->process = $process;
        $process->on('exit', $this->exited(...));
        $ack = $process->pipes[static::ACK_DESCRIPTOR];
        $ack->on('data', $this->receive(...));
        $ack->on('end', $ack->pause(...));
        $ack->on('error', $ack->pause(...));
        $this->exitMonitor = new PeriodicTimer($this->loop, 0.1, $this->pollExit(...));
        $process->stdin->on('drain', function (): void {
            $this->backpressured = false;
        });
    }

    /**
     * Close the pipes after exit even if a descendant holds the acknowledgement descriptor.
     */
    protected function pollExit(): void
    {
        $process = $this->process;

        if ($process === null || $process->isRunning()) {
            return;
        }

        $this->exitMonitor?->cancel();
        $this->exitMonitor = null;
        $process->stdin->close();

        $this->loop->futureTick(static function () use ($process): void {
            $ack = $process->pipes[static::ACK_DESCRIPTOR];
            $ack->pause();
            $ack->close();
        });
    }

    /**
     * Determine whether the worker is still active.
     */
    public function running(): bool
    {
        return $this->process !== null;
    }

    /**
     * Start timers for enabled features and send the initial metrics snapshot.
     */
    public function start(Metrics $metrics): void
    {
        if (! $this->running() || $this->metrics !== null || $this->stopping !== null) {
            return;
        }

        $this->metrics = $metrics;

        if ($this->lifecycle !== null) {
            $this->recorder->whenBatchReady($this->lifecycle->batch, $this->scheduleLifecycle(...));

            if ($this->recorder->pending($this->lifecycle->batch)) {
                $this->scheduleLifecycle();
            }

            $this->lifecycleInterval = new PeriodicTimer($this->loop, $this->lifecycle->interval, function (): void {
                $this->lifecycleDue = $this->recorder->pending();
                $this->flush();
            });
        }

        if ($this->config !== null) {
            $this->interval = new PeriodicTimer($this->loop, $this->config->interval, $this->scheduleMetrics(...));
            $this->scheduleMetrics();
        }
    }

    /**
     * Schedule one flush for a full batch without writing from the enqueue path.
     */
    protected function scheduleLifecycle(): void
    {
        if ($this->flushScheduled || ! $this->available()) {
            return;
        }

        $this->flushScheduled = true;
        $this->loop->futureTick(function (): void {
            $this->flushScheduled = false;
            $this->flush();
        });
    }

    /**
     * Mark one metrics snapshot due, skipping busy ticks when only metrics are enabled.
     */
    protected function scheduleMetrics(): void
    {
        if ($this->available() || $this->lifecycle !== null) {
            $this->metricsDue = true;
            $this->flush();
        }
    }

    /**
     * Take turns sending due messages when both features need the free slot.
     */
    protected function flush(): void
    {
        if ($this->inputClosing || ! $this->available()) {
            return;
        }

        $lifecycleDue = $this->recorder?->pending($this->lifecycleDue || $this->drained ? 1 : $this->lifecycle->batch);

        if ($lifecycleDue && (! $this->metricsDue || $this->lastSent === 'metrics')) {
            $this->lifecycleDue = false;
            $this->sendLifecycle();
        } elseif ($this->metricsDue) {
            $this->metricsDue = false;
            $this->record($this->metrics);
        }

        if ($this->drained) {
            if (! $this->recorder->pending()) {
                $this->finishInput();
            } elseif ($this->pending === null) {
                $this->loop->futureTick($this->flush(...));
            }
        }
    }

    /**
     * Hand one bounded batch to the worker and count its records.
     */
    protected function sendLifecycle(): void
    {
        [$payload, $sent, $dropped] = $this->recorder->batch($this->node, $this->lifecycle->batch, static::MAX_LINE_BYTES);

        $this->metrics?->increment('murmur_lifecycle_dropped_total', $dropped);

        if ($sent > 0) {
            $this->pendingLifecycle = $sent;
            $this->send('lifecycle', $payload, $this->lifecycle->timeout);
            $this->metrics?->increment('murmur_lifecycle_sent_total', $sent);
        }
    }

    /**
     * Stop metrics writes and reap the worker within the existing shutdown budget.
     *
     * @param  PromiseInterface<null>|null  $drained
     * @return PromiseInterface<null>
     */
    public function stop(?PromiseInterface $drained = null, float $graceSeconds = 1.0): PromiseInterface
    {
        if ($this->stopping === null) {
            $this->stopping = new Deferred;
            $draining = $this->recorder !== null && $drained !== null;
            $this->cancelWrites($draining);
            $this->metricsDue = false;

            if ($this->process === null) {
                $this->stopping->resolve(null);
            } else {
                $this->shutdownTimer = $this->loop->addTimer($draining ? $graceSeconds : 1.0, $this->terminate(...));

                if ($draining) {
                    $drained->then(fn () => $this->loop->futureTick($this->finishDraining(...)));
                } elseif ($this->recorder !== null) {
                    $this->finishDraining();
                } else {
                    $this->finishInput();
                }
            }
        }

        return $this->stopping->promise();
    }

    /**
     * Mark all queued lifecycle records due and flush until the queue is empty.
     */
    protected function finishDraining(): void
    {
        $this->drained = true;
        $this->flush();
    }

    /**
     * Stop scheduling messages and signal EOF after any buffered write.
     */
    protected function finishInput(): void
    {
        $this->cancelWrites();
        $this->inputClosing = true;
        $this->process?->stdin->end();
    }

    /**
     * Count queued and unacknowledged lifecycle records when the worker stops.
     */
    protected function discardLifecycle(): void
    {
        if ($this->recorder !== null) {
            $dropped = $this->recorder->stop() + $this->pendingLifecycle;
            $this->pendingLifecycle = 0;
            $this->metrics?->increment('murmur_lifecycle_dropped_total', $dropped);

            if ($dropped > 0 && $this->stopping !== null) {
                $records = $dropped === 1 ? 'record' : 'records';
                ErrorReporter::report(new RuntimeException("Murmur dropped {$dropped} lifecycle {$records} during shutdown."), force: true);
            }
        }
    }

    /**
     * Send a bounded frame only after the last snapshot has been acknowledged.
     */
    protected function record(Metrics $metrics): void
    {
        if (! $this->available()) {
            return;
        }

        try {
            $samples = $metrics->snapshot();

            if ($this->config->only !== []) {
                $samples = array_values(array_filter($samples, fn (array $sample): bool => in_array($sample['name'], $this->config->only, true)));
            }

            $payload = json_encode([
                'type' => 'metrics',
                'node' => $this->node,
                'recorded_at' => time(),
                'samples' => Snapshot::encodeSamples($samples),
            ], JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION);
        } catch (Throwable) {
            ErrorReporter::report(new RuntimeException('Murmur could not encode the metrics snapshot.'));

            return;
        }

        if (strlen($payload) > static::MAX_LINE_BYTES) {
            ErrorReporter::report(new RuntimeException('Murmur metrics snapshot exceeds the worker input limit.'));

            return;
        }

        $this->send('metrics', $payload, $this->config->timeout);
    }

    /**
     * Determine whether a new message can be sent without queueing in the pipe.
     */
    protected function available(): bool
    {
        return $this->process !== null && $this->pending === null && ! $this->backpressured
            && $this->process->stdin->isWritable();
    }

    /**
     * Hold the write slot until the worker answers, even after reporting a timeout.
     */
    protected function send(string $type, string $payload, float $timeout): void
    {
        $this->pending = $this->lastSent = $type;

        if ($this->stopping === null) {
            $this->deadline = $this->loop->addTimer($timeout, function (TimerInterface $timer) use ($type): void {
                if ($this->deadline !== $timer) {
                    return;
                }

                $this->deadline = null;
                ErrorReporter::report(new RuntimeException("Murmur {$type} recorder timed out."));
            });
        }
        $this->backpressured = ! $this->process->stdin->write($payload."\n");
    }

    /**
     * Parse acknowledgements incrementally without retaining an over-long line.
     */
    protected function receive(string $chunk): void
    {
        if ($this->inputClosing && $this->pending !== 'lifecycle') {
            return;
        }

        $offset = 0;
        $length = strlen($chunk);

        while ($offset < $length) {
            $newline = strpos($chunk, "\n", $offset);
            $size = ($newline === false ? $length : $newline) - $offset;

            if ($this->reply !== null) {
                if (strlen($this->reply) + $size > static::ACK_BYTES) {
                    $this->reply = null;
                } else {
                    $this->reply .= substr($chunk, $offset, $size);
                }
            }

            if ($newline === false) {
                return;
            }

            $this->acknowledge($this->reply ?? '');
            $this->reply = '';
            $offset = $newline + 1;
        }

        $this->flush();
    }

    /**
     * Release the write slot for any line that answers a delivered message.
     *
     * A malformed line counts as a failed write and releases a delivered message's slot.
     */
    protected function acknowledge(string $reply): void
    {
        $valid = in_array($reply, ['ok', 'error'], true);

        if (! $valid) {
            ErrorReporter::report(new RuntimeException('Murmur worker sent an invalid acknowledgement.'));
        }

        if ($this->pending === null || $this->backpressured) {
            if ($valid) {
                ErrorReporter::report(new RuntimeException('Murmur worker sent an unexpected acknowledgement.'));
            }

            return;
        }

        $type = $this->pending;
        $this->pending = null;
        $this->pendingLifecycle = 0;
        $this->cancelDeadline();

        if ($reply === 'error') {
            $message = $type === 'metrics' ? 'write the metrics snapshot' : "process the {$type} message";
            ErrorReporter::report(new RuntimeException("Murmur could not {$message}."));
        }
    }

    /**
     * Stop writes, clear the worker state, and resolve shutdown after ReactPHP reaps it.
     */
    protected function exited(?int $code, ?int $signal): void
    {
        $this->process = null;

        $this->exitMonitor?->cancel();
        $this->exitMonitor = null;

        if ($this->shutdownTimer !== null) {
            $this->loop->cancelTimer($this->shutdownTimer);
            $this->shutdownTimer = null;
        }

        $this->cancelWrites();
        $this->discardLifecycle();

        if ($this->stopping !== null) {
            $this->stopping->resolve(null);

            return;
        }

        $status = $signal === null ? 'exit code '.($code ?? 'unknown') : "signal {$signal}";
        ErrorReporter::report(new RuntimeException("Murmur worker recording has stopped until the server restarts (worker {$status})."), force: true);
    }

    /**
     * Cancel scheduled writes and deadlines, keeping lifecycle ticks while streams drain.
     */
    protected function cancelWrites(bool $draining = false): void
    {
        $this->interval?->cancel();
        $this->interval = null;

        if (! $draining) {
            $this->lifecycleInterval?->cancel();
            $this->lifecycleInterval = null;
        }

        $this->cancelDeadline();
    }

    /**
     * Cancel the timeout for the current message.
     */
    protected function cancelDeadline(): void
    {
        if ($this->deadline !== null) {
            $this->loop->cancelTimer($this->deadline);
            $this->deadline = null;
        }
    }

    /**
     * Signal a worker that has not exited after EOF and escalate if necessary.
     */
    protected function terminate(TimerInterface $timer): void
    {
        $process = $this->process;

        if ($process === null || $this->shutdownTimer !== $timer) {
            return;
        }

        $this->discardLifecycle();
        $this->cancelWrites();
        $this->inputClosing = true;

        $process->stdin->close();
        $process->terminate();
        $this->shutdownTimer = $this->loop->addTimer(0.2, fn () => $process->terminate(static::KILL_SIGNAL));
    }
}
