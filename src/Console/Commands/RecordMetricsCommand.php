<?php

namespace Arttecs\Murmur\Console\Commands;

use Arttecs\Murmur\Contracts\MetricsStore;
use Arttecs\Murmur\Metrics\Snapshot;
use Arttecs\Murmur\MurmurManager;
use Arttecs\Murmur\Server\Metrics;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use InvalidArgumentException;
use stdClass;
use Throwable;

/**
 * Read and store snapshots from the server until standard input closes.
 */
class RecordMetricsCommand extends Command
{
    /**
     * The command name used by the server's recorder.
     */
    protected $signature = 'murmur:record-metrics';

    /**
     * The description of the internal command.
     */
    protected $description = 'Record Murmur metrics snapshots from standard input';

    /**
     * Hide the writer from the Artisan command list.
     */
    protected $hidden = true;

    /**
     * Resolve the store once and acknowledge each input line.
     */
    public function handle(MurmurManager $manager): int
    {
        $this->watchParent();
        ob_start(static fn (): string => '', 8192);

        try {

            try {
                $store = $manager->metricsStore();
            } catch (InvalidArgumentException) {
                return $this->failure('Murmur metrics store configuration is invalid.');
            } catch (Throwable) {
                return $this->failure('Murmur could not initialize the metrics store.');
            }

            if ($store === null) {
                return $this->failure('No Murmur metrics store is configured.');
            }

            $this->clearQueryLogs();

            while (($line = @fgets(STDIN, Snapshot::MAX_DOCUMENT_BYTES + 2)) !== false) {
                if (str_ends_with($line, "\n")) {
                    $saved = $this->writeSnapshot($store, $line);
                } else {
                    unset($line);
                    $this->discardLine();
                    $saved = false;
                }

                if (@fwrite(STDOUT, $saved ? "ok\n" : "error\n") === false || ! @fflush(STDOUT)) {
                    break;
                }
            }
        } finally {
            if (function_exists('pcntl_alarm')) {
                pcntl_alarm(0);
            }

            ob_end_clean();
        }

        return self::SUCCESS;
    }

    /**
     * Skip the rest of a rejected input line using bounded reads.
     */
    protected function discardLine(): void
    {
        while (($chunk = @fgets(STDIN, 8192)) !== false && ! str_ends_with($chunk, "\n")) {
        }
    }

    /**
     * Validate and store one snapshot, reporting store failures through Laravel.
     */
    protected function writeSnapshot(MetricsStore $store, string $line): bool
    {
        $data = json_decode($line);

        if (! $data instanceof stdClass || count((array) $data) !== 3 || ! Snapshot::validNode($data->node ?? null)) {
            return false;
        }

        $snapshot = Snapshot::fromDecoded($data->recorded_at ?? null, $data->samples ?? null);

        if ($snapshot === null || array_diff(array_column($snapshot['samples'], 'name'), Metrics::names()) !== []) {
            return false;
        }

        try {
            $store->put($data->node, $snapshot['recorded_at'], $snapshot['samples']);
        } catch (Throwable $error) {
            try {
                report($error);
            } catch (Throwable) {
            }

            return false;
        } finally {
            $this->clearQueryLogs();
        }

        return true;
    }

    /**
     * Disable and clear query logs on Laravel database connections used by the writer.
     */
    protected function clearQueryLogs(): void
    {
        if (! app()->resolved('db')) {
            return;
        }

        foreach (DB::getConnections() as $connection) {
            $connection->disableQueryLog();
            $connection->flushQueryLog();
        }
    }

    /**
     * Let the server handle Ctrl-C and detect parent death during a store call.
     */
    protected function watchParent(): void
    {
        if (! function_exists('pcntl_signal')) {
            return;
        }

        if (! stream_isatty(STDIN)) {
            pcntl_signal(SIGINT, SIG_IGN);
        }

        if (! function_exists('posix_getppid') || ! function_exists('pcntl_alarm') || ! function_exists('pcntl_async_signals')) {
            return;
        }

        $parent = posix_getppid();
        pcntl_async_signals(true);
        pcntl_signal(SIGALRM, static function () use ($parent): void {
            if (posix_getppid() !== $parent) {
                exit(self::SUCCESS);
            }

            pcntl_alarm(1);
        }, restart_syscalls: false);

        pcntl_alarm(1);
    }

    /**
     * Report a single startup error line to standard error.
     */
    protected function failure(string $message): int
    {
        fwrite(STDERR, $message."\n");

        return self::FAILURE;
    }
}
