<?php

namespace Arttecs\Murmur\Console\Commands;

use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\MurmurServiceProvider;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Server\Log\RedisEventLog;
use Arttecs\Murmur\Server\Server;
use Arttecs\Murmur\Server\ServerConfig;
use Illuminate\Console\Command;
use InvalidArgumentException;
use React\EventLoop\Loop;
use Throwable;

/**
 * Start the Murmur SSE server from a Laravel application.
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
        try {
            $config = $this->makeConfig();
            $log = $this->makeLog();
        } catch (InvalidArgumentException $exception) {
            $this->error($exception->getMessage());

            return self::FAILURE;
        }

        $loop = Loop::get();
        $server = new Server($config, $log, $loop);
        $signals = extension_loaded('pcntl') ? [SIGTERM, SIGINT] : [];
        $shuttingDown = false;
        $shutdown = function () use ($server, $loop, &$shuttingDown): void {
            $shuttingDown = true;
            $server->shutdown()->then($loop->stop(...));
        };

        $exitCode = self::SUCCESS;

        try {
            $server->listen()->then(function () use ($server): void {
                $this->info('Murmur is listening on '.$server->address());
            }, function (Throwable $error) use (&$exitCode, $loop, &$shuttingDown): void {
                if ($shuttingDown) {
                    return;
                }

                $this->error($error->getMessage());
                $exitCode = self::FAILURE;
                $loop->futureTick($loop->stop(...));
            });

            foreach ($signals as $signal) {
                $loop->addSignal($signal, $shutdown);
            }

            $loop->run();
        } finally {
            foreach ($signals as $signal) {
                $loop->removeSignal($signal, $shutdown);
            }

            $server->close();
        }

        return $exitCode;
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
        );
    }

    /**
     * Create the event log for the configured server driver.
     *
     * @throws InvalidArgumentException
     */
    public function makeLog(): EventLog
    {
        $driver = config('murmur.driver');

        return match ($driver) {
            'http' => new MemoryEventLog((int) config('murmur.server.log_size')),
            'redis' => new RedisEventLog(
                config('murmur.redis.url'),
                MurmurServiceProvider::fullStreamKey(),
                config('murmur.server.log_size'),
                maxPayloadBytes: config('murmur.server.max_payload_bytes'),
                queryTimeoutSeconds: config('murmur.redis.query_timeout'),
                blockMs: config('murmur.redis.block_ms'),
            ),
            default => throw new InvalidArgumentException("Unsupported Murmur server driver [{$driver}]."),
        };
    }
}
