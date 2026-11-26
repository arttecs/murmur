<?php

namespace Arttecs\Murmur\Console\Commands;

use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\Metrics\MetricsRecorder;
use Arttecs\Murmur\MurmurManager;
use Arttecs\Murmur\Server\LoopBackend;
use Arttecs\Murmur\Server\Server;
use Arttecs\Murmur\Server\ServerConfig;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\ProcessUtils;
use InvalidArgumentException;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use RuntimeException;
use Symfony\Component\Console\Input\ArgvInput;
use Throwable;

use function Illuminate\Support\artisan_binary;
use function React\Promise\all;

/**
 * Start the Murmur SSE server from a Laravel application.
 *
 * @internal
 */
class ServeCommand extends Command
{
    /**
     * The command name and its listening address options.
     */
    protected $signature = 'murmur:serve {--host=} {--port=}';

    /**
     * The command description shown by Artisan.
     */
    protected $description = 'Start the Murmur SSE server';

    /**
     * Start the server and run its event loop until shutdown.
     */
    public function handle(): int
    {
        $loop = Loop::get();
        $manager = app(MurmurManager::class);
        $backend = new LoopBackend($loop::class);

        try {
            $config = $this->makeConfig();
            $backend->validate($config->maxSockets);
            $metrics = $manager->metricsConfiguration();
            $host = gethostname();
            $recorder = $metrics === null ? null : new MetricsRecorder(
                $loop,
                $metrics,
                $metrics->node ?? ($host === false ? '' : $host.':'.$config->port),
                $this->metricsCommand(),
            );
        } catch (InvalidArgumentException $exception) {
            $this->error($exception->getMessage());

            return self::FAILURE;
        }

        $channel = config('murmur.server.log_channel');

        if ($channel !== null && config("logging.channels.{$channel}") === null) {
            $this->error("Log channel [{$channel}] is not defined.");

            return self::FAILURE;
        }

        try {
            $recorder?->launch();
        } catch (RuntimeException $exception) {
            $this->error($exception->getMessage());

            return self::FAILURE;
        }

        if (($notice = $backend->notice()) !== null) {
            $this->info($notice);
        }

        $server = null;
        $signals = [];
        $shuttingDown = false;
        $exitCode = self::SUCCESS;

        try {
            try {
                $log = $this->makeLog();
            } catch (InvalidArgumentException $exception) {
                $this->error($exception->getMessage());

                return self::FAILURE;
            }

            $server = new Server($config, $log, $loop, $channel === null ? null : Log::channel($channel));
            $manager->callServingCallbacks($server, $loop);

            $signals = extension_loaded('pcntl') ? [SIGTERM, SIGINT] : [];
            $shutdown = function () use ($server, $recorder, $loop, &$shuttingDown): void {
                $shuttingDown = true;

                if ($recorder === null) {
                    $server->shutdown()->then($loop->stop(...));

                    return;
                }

                all([$recorder->stop(), $server->shutdown()])->then($loop->stop(...));
            };

            foreach ($signals as $signal) {
                $loop->addSignal($signal, $shutdown);
            }

            $server->listen()->then(function () use ($server, $recorder): void {
                $this->info('Murmur is listening on '.$server->address());
                $recorder?->start($server->metrics());
            }, function (Throwable $error) use (&$exitCode, $loop, &$shuttingDown): void {
                if ($shuttingDown) {
                    return;
                }

                $this->error($error->getMessage());
                $exitCode = self::FAILURE;
                $loop->futureTick($loop->stop(...));
            });

            $loop->run();
        } finally {
            foreach ($signals as $signal) {
                $loop->removeSignal($signal, $shutdown);
            }

            try {
                $server?->close();
            } finally {
                if ($recorder !== null) {
                    $this->stopRecorder($recorder, $loop);
                }
            }
        }

        return $exitCode;
    }

    /**
     * Build the writer command using the running PHP and Artisan entry point.
     *
     * @throws InvalidArgumentException
     */
    protected function metricsCommand(): string
    {
        $artisan = artisan_binary();

        if ($artisan === 'artisan') {
            $artisan = $this->input instanceof ArgvInput && isset($_SERVER['SCRIPT_FILENAME'])
                ? $_SERVER['SCRIPT_FILENAME']
                : base_path('artisan');
        }

        $path = realpath($artisan);

        if ($path === false || ! is_file($path)) {
            throw new InvalidArgumentException('Murmur metrics recording could not locate the Artisan entry point.');
        }

        return ProcessUtils::escapeArgument(PHP_BINARY).' '.ProcessUtils::escapeArgument($path).' murmur:record-metrics';
    }

    /**
     * Reap any writer still running if the event loop stopped outside graceful shutdown.
     */
    protected function stopRecorder(MetricsRecorder $recorder, LoopInterface $loop): void
    {
        $stopped = false;
        $shutdown = $recorder->stop()->then(function () use (&$stopped): void {
            $stopped = true;
        });

        if (! $stopped) {
            $shutdown->then($loop->stop(...));
            $cap = $loop->addTimer(2.0, $loop->stop(...));
            $loop->run();
            $loop->cancelTimer($cap);
        }
    }

    /**
     * Build the server configuration using Laravel settings and command options.
     *
     * @throws InvalidArgumentException
     */
    public function makeConfig(): ServerConfig
    {
        return new ServerConfig(
            secret: (string) config('murmur.secret'),
            host: (string) ($this->option('host') ?? config('murmur.server.host')),
            port: (int) ($this->option('port') ?? config('murmur.server.port')),
            heartbeatSeconds: (float) config('murmur.server.heartbeat'),
            retryMs: (int) config('murmur.server.retry_ms'),
            maxConnections: (int) config('murmur.server.max_connections'),
            maxTopicsPerConnection: (int) config('murmur.server.max_topics_per_connection'),
            maxPayloadBytes: (int) config('murmur.server.max_payload_bytes'),
            maxBufferBytes: (int) config('murmur.server.max_buffer_bytes'),
            allowedOrigins: config('murmur.server.allowed_origins'),
            publishEnabled: config('murmur.driver') !== 'redis',
            signatureToleranceSeconds: (int) config('murmur.server.signature_tolerance'),
            maxSockets: (int) config('murmur.server.max_sockets'),
            requestTimeoutSeconds: (float) config('murmur.server.request_timeout'),
            maxReplayEvents: (int) config('murmur.server.max_replay_events'),
            shutdownGraceSeconds: (float) config('murmur.server.shutdown_grace'),
            loopLagThresholdMs: (int) config('murmur.server.loop_lag_threshold_ms'),
            shutdownSpreadSeconds: (float) config('murmur.server.shutdown_spread'),
        );
    }

    /**
     * Create the event log for the configured server driver.
     *
     * @throws InvalidArgumentException
     */
    public function makeLog(): EventLog
    {
        return app(MurmurManager::class)->eventLog();
    }
}
