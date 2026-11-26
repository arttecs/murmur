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
 * Hand metrics snapshots to a short-lived Artisan process without waiting in the server loop.
 */
class MetricsRecorder
{
    /**
     * The Unix signal that forces a writer to exit after the termination grace period.
     */
    protected const KILL_SIGNAL = 9;

    /**
     * The timer that starts periodic writes.
     */
    protected ?TimerInterface $interval = null;

    /**
     * The timer that enforces the current write deadline.
     */
    protected ?TimerInterface $deadline = null;

    /**
     * The timer that escalates termination if the child is still alive.
     */
    protected ?TimerInterface $kill = null;

    /**
     * The child that currently owns the write slot.
     */
    protected ?Process $process = null;

    /**
     * The pending shutdown, resolved after the child has been reaped.
     *
     * @var Deferred<null>|null
     */
    protected ?Deferred $stopping = null;

    /**
     * The absolute path of the Bash binary that launches writers.
     */
    protected string $bash;

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

        if (! $this->descriptorsListable()) {
            throw new InvalidArgumentException('Murmur metrics recording requires a readable /proc/self/fd or /dev/fd.');
        }

        $this->bash = $this->bashBinary() ?? throw new InvalidArgumentException('Murmur metrics recording requires Bash. Install it or unset MURMUR_METRICS_STORE.');
    }

    /**
     * Check whether the launcher can list the descriptors it must close.
     */
    protected function descriptorsListable(): bool
    {
        return is_readable('/proc/self/fd') || is_readable('/dev/fd');
    }

    /**
     * Find the absolute path of an executable Bash binary.
     */
    protected function bashBinary(): ?string
    {
        $directories = [...explode(PATH_SEPARATOR, (string) getenv('PATH')), '/bin', '/usr/bin', '/usr/local/bin'];

        foreach ($directories as $directory) {
            $path = $directory.'/bash';

            if (str_starts_with($directory, '/') && is_file($path) && is_executable($path)) {
                return $path;
            }
        }

        return null;
    }

    /**
     * Record the first snapshot as soon as the server is listening.
     */
    public function start(Metrics $metrics): void
    {
        if ($this->interval !== null || $this->stopping !== null) {
            return;
        }

        $this->interval = $this->loop->addPeriodicTimer($this->config->interval, fn () => $this->record($metrics));
        $this->record($metrics);
    }

    /**
     * Cancel new writes and let the loop reap the current child.
     *
     * @return PromiseInterface<null>
     */
    public function stop(): PromiseInterface
    {
        if ($this->interval !== null) {
            $this->loop->cancelTimer($this->interval);
            $this->interval = null;
        }

        if ($this->stopping === null) {
            $this->stopping = new Deferred;

            if ($this->process === null) {
                $this->stopping->resolve(null);
            } else {
                $this->terminate();
            }
        }

        return $this->stopping->promise();
    }

    /**
     * Start a child only when the previous snapshot has finished writing.
     */
    protected function record(Metrics $metrics): void
    {
        if ($this->process !== null) {
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

            $process = new Process($this->isolatedCommand());
            $process->on('exit', $this->exited(...));
            $process->start($this->loop, 0.01);
        } catch (Throwable $error) {
            ErrorReporter::report(new RuntimeException('Murmur could not start the metrics recorder.', previous: $error));

            return;
        }

        $this->process = $process;
        $this->deadline = $this->loop->addTimer($this->config->timeout, function (): void {
            ErrorReporter::report(new RuntimeException('Murmur metrics recorder timed out.'));
            $this->terminate();
        });
        $process->stdin->end($payload);
    }

    /**
     * Close inherited descriptors in the launcher before replacing it with PHP.
     */
    protected function isolatedCommand(): string
    {
        $launcher = <<<'BASH'
        if [[ -r /proc/self/fd ]]; then
            directory=/proc/self/fd
        elif [[ -r /dev/fd ]]; then
            directory=/dev/fd
        else
            exit 1
        fi
        for path in "$directory"/*; do
            descriptor=${path##*/}
            case "$descriptor" in
                ''|*[!0-9]*|[012]) continue ;;
            esac
            eval "exec $descriptor>&-"
        done
        exec "$@"
        BASH;

        return 'unset BASH_ENV; exec '.escapeshellarg($this->bash).' -c '.escapeshellarg($launcher).' murmur-metrics '.$this->command;
    }

    /**
     * Release the write slot after ReactPHP has reaped the child.
     */
    protected function exited(?int $code, ?int $signal): void
    {
        if ($this->kill === null && $this->stopping === null && ($code !== 0 || $signal !== null)) {
            $status = $signal === null ? "exit code {$code}" : "signal {$signal}";
            ErrorReporter::report(new RuntimeException("Murmur metrics recorder exited unsuccessfully ({$status}). The cause is in the application's log."));
        }

        if ($this->deadline !== null) {
            $this->loop->cancelTimer($this->deadline);
            $this->deadline = null;
        }

        if ($this->kill !== null) {
            $this->loop->cancelTimer($this->kill);
            $this->kill = null;
        }

        $this->process = null;
        $this->stopping?->resolve(null);
    }

    /**
     * Signal the writer and escalate to a forced exit if it does not stop promptly.
     */
    protected function terminate(): void
    {
        if ($this->kill !== null) {
            return;
        }

        if ($this->deadline !== null) {
            $this->loop->cancelTimer($this->deadline);
            $this->deadline = null;
        }

        $process = $this->process;
        $this->kill = $this->loop->addTimer(0.1, fn () => $process->terminate(static::KILL_SIGNAL));
        $process->terminate();

        foreach ($process->pipes as $pipe) {
            $pipe->close();
        }
    }
}
