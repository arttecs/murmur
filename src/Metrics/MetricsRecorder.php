<?php

namespace Arttecs\Murmur\Metrics;

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
 * Send server snapshots to a single Artisan writer without waiting in the event loop.
 */
class MetricsRecorder
{
    /**
     * The Unix signal that forces a writer to exit.
     */
    protected const KILL_SIGNAL = 9;

    /**
     * The maximum length of a writer acknowledgement before its newline.
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
     * The writer started before the server opened any sockets.
     */
    protected ?Process $process = null;

    /**
     * Whether the previous snapshot still needs an acknowledgement.
     */
    protected bool $pending = false;

    /**
     * Whether the writer's input stream cannot accept more data.
     */
    protected bool $backpressured = false;

    /**
     * The partial acknowledgement, or null while discarding an over-long line.
     */
    protected ?string $reply = '';

    /**
     * The pending shutdown, resolved after the writer has been reaped.
     *
     * @var Deferred<null>|null
     */
    protected ?Deferred $stopping = null;

    /**
     * Prepare the recorder and verify that this host can launch and signal the writer.
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
     * Launch the writer before the server establishes any network connections.
     *
     * @throws RuntimeException
     */
    public function launch(): void
    {
        $process = new Process('exec '.$this->command);

        try {
            $process->start($this->loop, 0.01);
        } catch (RuntimeException $error) {
            throw new RuntimeException('Murmur could not start the metrics recorder.', previous: $error);
        }

        $this->process = $process;
        $process->on('exit', $this->exited(...));
        $process->stdout->on('data', $this->receive(...));
        $process->stderr->on('data', static function (): void {});
        $process->stdin->on('drain', function (): void {
            $this->backpressured = false;
        });
    }

    /**
     * Determine whether the writer is still active.
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
     * Stop scheduling snapshots, signal EOF, and let the loop reap the writer.
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
        if ($this->pending || $this->process === null) {
            return;
        }

        try {
            $samples = $metrics->snapshot();

            if ($this->config->only !== []) {
                $samples = array_values(array_filter($samples, fn (array $sample): bool => in_array($sample['name'], $this->config->only, true)));
            }

            $payload = json_encode([
                'node' => $this->node,
                'recorded_at' => time(),
                'samples' => Snapshot::encodeSamples($samples),
            ], JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION);
        } catch (Throwable) {
            ErrorReporter::report(new RuntimeException('Murmur could not encode the metrics snapshot.'));

            return;
        }

        if (strlen($payload) > Snapshot::MAX_DOCUMENT_BYTES) {
            ErrorReporter::report(new RuntimeException('Murmur metrics snapshot exceeds the writer input limit.'));

            return;
        }

        $this->pending = true;
        $this->deadline = $this->loop->addTimer($this->config->timeout, function (): void {
            $this->deadline = null;
            ErrorReporter::report(new RuntimeException('Murmur metrics recorder timed out.'));
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
     * Release the write slot for any line that answers a delivered snapshot.
     *
     * A line that is not an acknowledgement counts as a failed write, so output
     * mixed into the reply cannot leave the slot pending forever.
     */
    protected function acknowledge(string $reply): void
    {
        $valid = in_array($reply, ['ok', 'error'], true);

        if (! $valid) {
            ErrorReporter::report(new RuntimeException('Murmur metrics recorder sent an invalid acknowledgement.'));
        }

        if (! $this->pending || $this->backpressured) {
            if ($valid) {
                ErrorReporter::report(new RuntimeException('Murmur metrics recorder sent an unexpected acknowledgement.'));
            }

            return;
        }

        $this->pending = false;
        $this->cancelDeadline();

        if ($reply === 'error') {
            ErrorReporter::report(new RuntimeException('Murmur could not write the metrics snapshot. The writer reported the cause through the exception handler.'));
        }
    }

    /**
     * Stop writes, clear the writer state, and resolve shutdown after ReactPHP reaps it.
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
        ErrorReporter::report(new RuntimeException("Murmur metrics recording has stopped until the server restarts (writer {$status})."), force: true);
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
     * Signal a writer that has not exited after EOF and escalate if necessary.
     */
    protected function terminate(): void
    {
        $process = $this->process;
        $this->shutdownTimer = $this->loop->addTimer(0.2, function () use ($process): void {
            $process->terminate(static::KILL_SIGNAL);
            $process->stdout->close();
            $process->stderr->close();
        });
        $process->terminate();
    }
}
