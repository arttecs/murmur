<?php

namespace Arttecs\Murmur\Console\Commands;

use Arttecs\Murmur\Contracts\MetricsStore;
use Arttecs\Murmur\Events\ConnectionClosed;
use Arttecs\Murmur\Events\ConnectionOpened;
use Arttecs\Murmur\Metrics\Snapshot;
use Arttecs\Murmur\MurmurManager;
use Arttecs\Murmur\Server\ConnectionId;
use Arttecs\Murmur\Server\Metrics;
use Arttecs\Murmur\Server\Topic;
use Arttecs\Murmur\Worker\LifecycleConfig;
use Arttecs\Murmur\Worker\Worker;
use Illuminate\Console\Command;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Log\Context\Repository as ContextRepository;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Facade;
use InvalidArgumentException;
use RuntimeException;
use stdClass;
use Throwable;

/**
 * Process typed messages from the server until standard input closes.
 *
 * @internal
 */
class WorkerCommand extends Command
{
    /**
     * The command name used to launch the worker.
     */
    protected $signature = 'murmur:worker';

    /**
     * The description of the internal command.
     */
    protected $description = 'Process Murmur worker messages from standard input';

    /**
     * Hide the worker from the Artisan command list.
     */
    protected $hidden = true;

    /**
     * Resolve the store once and acknowledge each input line.
     */
    public function handle(MurmurManager $manager, Dispatcher $events): int
    {
        $ack = @fopen('php://fd/'.Worker::ACK_DESCRIPTOR, 'w');

        if ($ack === false || ((fstat($ack)['mode'] ?? 0) & 0170000) !== 0010000) {
            return $this->failure('Murmur worker requires an acknowledgement pipe on descriptor 3.');
        }

        $this->watchParent();

        try {
            try {
                $lifecycle = LifecycleConfig::from(config());
            } catch (InvalidArgumentException $error) {
                return $this->failure($error->getMessage());
            }

            try {
                $store = $manager->metricsStore();
            } catch (InvalidArgumentException) {
                return $this->failure('Murmur metrics store configuration is invalid.');
            } catch (Throwable) {
                return $this->failure('Murmur could not initialize the metrics store.');
            }

            if ($store === null && $lifecycle === null) {
                return $this->failure('Murmur worker requires a metrics store or lifecycle events.');
            }

            $this->resetDatabaseConnections();

            while (($line = @fgets(STDIN, Worker::MAX_LINE_BYTES + 2)) !== false) {
                if (str_ends_with($line, "\n")) {
                    $saved = $this->processMessage($store, $lifecycle, $events, json_decode($line));
                } else {
                    unset($line);
                    $this->discardLine();
                    $saved = false;
                }

                if (@fwrite($ack, $saved ? "ok\n" : "error\n") === false || ! @fflush($ack)) {
                    break;
                }
            }
        } finally {
            if (function_exists('pcntl_alarm')) {
                pcntl_alarm(0);
            }

            fclose($ack);
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
     * Route an input object by message type.
     */
    protected function processMessage(?MetricsStore $store, ?LifecycleConfig $lifecycle, Dispatcher $events, mixed $data): bool
    {
        if (! $data instanceof stdClass) {
            return false;
        }

        try {
            return match ($data->type ?? null) {
                'metrics' => $store !== null && $this->writeSnapshot($store, $data),
                'lifecycle' => $lifecycle !== null && $this->dispatchBatch($events, $data, $lifecycle->batch),
                default => false,
            };
        } catch (Throwable $error) {
            $this->reportFailure($error);

            return false;
        } finally {
            $this->resetMessageState();
        }
    }

    /**
     * Validate and store one snapshot, reporting store failures through Laravel.
     */
    protected function writeSnapshot(MetricsStore $store, stdClass $data): bool
    {
        if (count((array) $data) !== 4 || ! Snapshot::validNode($data->node ?? null)) {
            return false;
        }

        $snapshot = Snapshot::fromDecoded($data->recorded_at ?? null, $data->samples ?? null);

        if ($snapshot === null || array_diff(array_column($snapshot['samples'], 'name'), Metrics::names()) !== []) {
            return false;
        }

        $store->put($data->node, $snapshot['recorded_at'], $snapshot['samples']);

        return true;
    }

    /**
     * Validate the envelope and dispatch valid events in record order.
     */
    protected function dispatchBatch(Dispatcher $events, stdClass $data, int $batchLimit): bool
    {
        $records = $data->records ?? null;

        if (count((array) $data) !== 3 || ! Snapshot::validNode($data->node ?? null) ||
            ! is_array($records) || $records === [] || count($records) > $batchLimit) {
            return false;
        }

        $topicLimit = (int) config('murmur.server.max_topics_per_connection');
        $invalid = 0;
        $failures = 0;

        foreach ($records as $record) {
            if (! $record instanceof stdClass || ($event = $this->lifecycleEvent($data->node, $record, $topicLimit)) === null) {
                $invalid++;

                continue;
            }

            try {
                $events->dispatch($event);
            } catch (Throwable $error) {
                if (++$failures === 1) {
                    $this->reportFailure($error);
                }
            }
        }

        if ($invalid > 0) {
            $recordLabel = $invalid === 1 ? 'record' : 'records';
            $this->reportFailure(new RuntimeException("Murmur lifecycle batch had {$invalid} invalid {$recordLabel}."));
        }

        if ($failures > 0) {
            $eventLabel = $failures === 1 ? 'event' : 'events';
            fwrite(STDERR, "Murmur lifecycle batch had {$failures} failed {$eventLabel}.\n");
        }

        return $invalid === 0 && $failures === 0;
    }

    /**
     * Report an application failure without letting its handler interrupt the worker.
     */
    protected function reportFailure(Throwable $error): void
    {
        try {
            report($error);
        } catch (Throwable) {
        }
    }

    /**
     * Validate the fields and convert one record to a Laravel event.
     */
    protected function lifecycleEvent(string $node, stdClass $record, int $topicLimit): ConnectionOpened|ConnectionClosed|null
    {
        $id = $record->connectionId ?? null;
        $occurredAt = $record->occurredAt ?? null;
        $topics = $record->topics ?? null;

        if (! is_string($id) || ! ConnectionId::isValid($id) ||
            ! is_int($occurredAt) || $occurredAt < 1 ||
            ! is_array($topics) || $topics === [] || count($topics) > $topicLimit) {
            return null;
        }

        $decoded = [];

        foreach ($topics as $encoded) {
            if (! is_string($encoded) || ($topic = base64_decode($encoded, true)) === false ||
                base64_encode($topic) !== $encoded || ! Topic::isValid($topic) || isset($decoded[$encoded])) {
                return null;
            }

            $decoded[$encoded] = $topic;
        }

        $topics = array_values($decoded);
        $size = count((array) $record);

        if ($size === 3) {
            return new ConnectionOpened($id, $node, $topics, $occurredAt);
        }

        $duration = $record->durationSeconds ?? null;
        $reason = $record->reason ?? null;

        if ($size !== 5 || (! is_int($duration) && ! is_float($duration)) || ! is_finite($duration) || $duration < 0 ||
            ! in_array($reason, ['client', 'expired', 'slow_consumer', 'shutdown', 'error'], true)) {
            return null;
        }

        return new ConnectionClosed($id, $node, $topics, $occurredAt, (float) $duration, $reason);
    }

    /**
     * Reset per-message database, logging, container, and memory state.
     */
    protected function resetMessageState(): void
    {
        $this->resetDatabaseConnections();
        $app = app();

        if ($app->resolved('log')) {
            $log = $app->make('log');

            if (method_exists($log, 'flushSharedContext')) {
                $log->flushSharedContext();
            }

            if (method_exists($log, 'withoutContext')) {
                $log->withoutContext();
            }
        }

        if ($app->resolved(ContextRepository::class)) {
            $context = $app->make(ContextRepository::class);

            if (method_exists($context, 'flush')) {
                $context->flush();
            }
        }

        $app->forgetScopedInstances();
        Facade::clearResolvedInstances();

        if (function_exists('memory_reset_peak_usage')) {
            memory_reset_peak_usage();
        }
    }

    /**
     * Reset query logging and duration tracking on existing database connections.
     */
    protected function resetDatabaseConnections(): void
    {
        if (! app()->resolved('db')) {
            return;
        }

        foreach (DB::getConnections() as $connection) {
            $connection->disableQueryLog();
            $connection->flushQueryLog();

            if (method_exists($connection, 'resetTotalQueryDuration')) {
                $connection->resetTotalQueryDuration();
            }

            if (method_exists($connection, 'allowQueryDurationHandlersToRunAgain')) {
                $connection->allowQueryDurationHandlersToRunAgain();
            }
        }
    }

    /**
     * Let the server handle Ctrl-C and detect parent death while processing a message.
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
