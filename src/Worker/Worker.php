<?php

namespace Arttecs\Murmur\Worker;

use Arttecs\Murmur\Metrics\MetricsConfig;
use Arttecs\Murmur\Metrics\Snapshot;
use Arttecs\Murmur\Server\ErrorReporter;
use Arttecs\Murmur\Server\Metrics;
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
    protected ?TimerInterface $interval = null;

    /**
     * The timer that reports a late acknowledgement.
     */
    protected ?TimerInterface $deadline = null;

    /**
     * The timer that advances shutdown from EOF to termination or a forced exit.
     */
    protected ?TimerInterface $shutdownTimer = null;

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
     * Prepare the recorder and verify that this host can launch and signal the worker.
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        protected LoopInterface $loop,
        protected MetricsConfig $config,
        protected string $node,
        protected string $command,
    ) {
        if (! Snapshot::validNode($node)) {
            throw new InvalidArgumentException('Murmur could not derive a valid metrics node name. Set MURMUR_NODE explicitly.');
        }

        if (PHP_OS_FAMILY === 'Windows' || ! function_exists('proc_open')) {
            throw new InvalidArgumentException('Murmur metrics recording requires proc_open() on Linux or macOS.');
        }

        if (Process::isSigchildEnabled()) {
            throw new InvalidArgumentException('Murmur metrics recording requires PHP built without --enable-sigchild.');
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
        $process = new Process('exec '.$this->command, null, null, [['pipe', 'r'], ['pipe', 'w'], STDERR]);

        try {
            $process->start($this->loop, 0.01);
        } catch (RuntimeException $error) {
            throw new RuntimeException('Murmur could not start the metrics recorder.', previous: $error);
        }

        $this->process = $process;
        $process->on('exit', $this->exited(...));
        $process->stdout->on('data', $this->receive(...));
        $process->stdin->on('drain', function (): void {
            $this->backpressured = false;
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
     * Record the first snapshot as soon as the server is listening.
     */
    public function start(Metrics $metrics): void
    {
        if (! $this->running() || $this->interval !== null || $this->stopping !== null) {
            return;
        }

        $this->interval = $this->loop->addPeriodicTimer($this->config->interval, fn () => $this->record($metrics));
        $this->record($metrics);
    }

    /**
     * Stop scheduling snapshots, signal EOF, and let the loop reap the worker.
     *
     * @return PromiseInterface<null>
     */
    public function stop(): PromiseInterface
    {
        if ($this->stopping === null) {
            $this->stopping = new Deferred;
            $this->cancelWrites();

            if ($this->process === null) {
                $this->stopping->resolve(null);
            } else {
                $this->shutdownTimer = $this->loop->addTimer(1.0, $this->terminate(...));
                $this->process->stdin->end();
            }
        }

        return $this->stopping->promise();
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
        return $this->process !== null && $this->pending === null && ! $this->backpressured;
    }

    /**
     * Hold the write slot until the worker answers, even after reporting a timeout.
     */
    protected function send(string $type, string $payload, float $timeout): void
    {
        $this->pending = $type;
        $this->deadline = $this->loop->addTimer($timeout, function () use ($type): void {
            $this->deadline = null;
            ErrorReporter::report(new RuntimeException("Murmur {$type} recorder timed out."));
        });
        $this->backpressured = ! $this->process->stdin->write($payload."\n");
    }

    /**
     * Parse acknowledgements incrementally without retaining an over-long line.
     */
    protected function receive(string $chunk): void
    {
        if ($this->stopping !== null) {
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
    }

    /**
     * Release the write slot for any line that answers a delivered message.
     *
     * A line that is not an acknowledgement counts as a failed write, so output
     * mixed into the reply cannot leave the slot pending forever.
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
        $this->cancelDeadline();

        if ($reply === 'error') {
            $message = $type === 'metrics' ? 'write the metrics snapshot' : "process the {$type} message";
            ErrorReporter::report(new RuntimeException("Murmur could not {$message}. The worker reported the cause through the exception handler."));
        }
    }

    /**
     * Stop writes, clear the worker state, and resolve shutdown after ReactPHP reaps it.
     */
    protected function exited(?int $code, ?int $signal): void
    {
        $this->process = null;
        $this->cancelWrites();

        if ($this->shutdownTimer !== null) {
            $this->loop->cancelTimer($this->shutdownTimer);
            $this->shutdownTimer = null;
        }

        if ($this->stopping !== null) {
            $this->stopping->resolve(null);

            return;
        }

        $status = $signal === null ? 'exit code '.($code ?? 'unknown') : "signal {$signal}";
        ErrorReporter::report(new RuntimeException("Murmur worker recording has stopped until the server restarts (worker {$status})."), force: true);
    }

    /**
     * Cancel scheduled writes and the current acknowledgement deadline.
     */
    protected function cancelWrites(): void
    {
        if ($this->interval !== null) {
            $this->loop->cancelTimer($this->interval);
            $this->interval = null;
        }

        $this->cancelDeadline();
    }

    /**
     * Cancel the timeout for the current snapshot.
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
    protected function terminate(): void
    {
        $process = $this->process;
        $this->shutdownTimer = $this->loop->addTimer(0.2, function () use ($process): void {
            $process->terminate(static::KILL_SIGNAL);
            $process->stdout->close();
        });
        $process->terminate();
    }
}
