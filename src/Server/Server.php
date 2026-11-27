<?php

namespace Arttecs\Murmur\Server;

use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\Contracts\SubscriptionAuthorizer;
use Arttecs\Murmur\Server\Auth\JwtSubscriptionAuthorizer;
use Arttecs\Murmur\Server\Http\PublishHandler;
use Arttecs\Murmur\Server\Http\SseHandler;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Server\Sse\Frame;
use Closure;
use LogicException;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use Psr\Log\LoggerInterface;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\EventLoop\TimerInterface;
use React\Http\HttpServer;
use React\Http\Message\Response;
use React\Http\Middleware\StreamingRequestMiddleware;
use React\Promise\Deferred;
use React\Promise\PromiseInterface;
use React\Socket\ConnectionInterface;
use React\Socket\SocketServer;
use Throwable;

use function React\Promise\reject;
use function React\Promise\resolve;

/**
 * Serve Murmur's HTTP routes and manage the lifetime of its connections.
 */
class Server
{
    /**
     * The version of the Murmur wire protocol.
     */
    public const PROTOCOL = 1;

    /**
     * The seconds between event loop lag measurements.
     */
    protected const LAG_INTERVAL = 0.1;

    /**
     * The number of lag measurements summarized by the lag gauge.
     */
    protected const LAG_WINDOW_TICKS = 10;

    /**
     * The shortest interval in seconds between batches of streams ended during shutdown.
     */
    protected const DRAIN_INTERVAL = 0.01;

    /**
     * The loop used for HTTP and connection timers.
     */
    protected LoopInterface $loop;

    /**
     * The subscription authorizer used by the SSE handler.
     */
    protected SubscriptionAuthorizer $authorizer;

    /**
     * The hub shared by subscriptions and the event log.
     */
    protected Hub $hub;

    /**
     * The metrics collected by this server.
     */
    protected Metrics $metrics;

    /**
     * The monotonic timestamp when the server started listening.
     */
    protected int|float|null $startedAt = null;

    /**
     * The listening socket.
     */
    protected ?SocketServer $socket = null;

    /**
     * The pending or established listener.
     *
     * @var PromiseInterface<null>|null
     */
    protected ?PromiseInterface $listening = null;

    /**
     * The accepted HTTP sockets keyed by their local and remote addresses.
     *
     * @var array<string, ConnectionInterface>
     */
    protected array $clients = [];

    /**
     * The request deadlines keyed by client object ID.
     *
     * @var array<int, TimerInterface>
     */
    protected array $requestDeadlines = [];

    /**
     * The timer that sends periodic heartbeats.
     */
    protected ?PeriodicTimer $heartbeat = null;

    /**
     * The timer that measures event loop lag.
     */
    protected ?PeriodicTimer $lagTimer = null;

    /**
     * The largest event loop lag in seconds seen during the last completed measurement window.
     */
    protected float $loopLag = 0.0;

    /**
     * The monotonic timestamp of the last loop lag warning.
     */
    protected int|float|null $lastLagWarningAt = null;

    /**
     * The pending or completed graceful shutdown.
     *
     * @var Deferred<null>|null
     */
    protected ?Deferred $shutdown = null;

    /**
     * The deadline for closing any sockets left during shutdown.
     */
    protected ?TimerInterface $shutdownDeadline = null;

    /**
     * The timer that ends streams in batches during shutdown.
     */
    protected ?TimerInterface $streamEndTimer = null;

    /**
     * The error counter installed on the reporter while listening.
     *
     * @var Closure(): void|null
     */
    protected ?Closure $errorCounter = null;

    /**
     * Indicates whether the server has shut down.
     */
    protected bool $closed = false;

    /**
     * Create a server with an event log and optional loop, logger, subscription authorizer, and lifecycle recorder.
     */
    public function __construct(
        protected ServerConfig $config,
        protected EventLog $log,
        ?LoopInterface $loop = null,
        protected ?LoggerInterface $logger = null,
        ?SubscriptionAuthorizer $authorizer = null,
        ?LifecycleRecorder $lifecycle = null,
    ) {
        $this->authorizer = $authorizer ?? new JwtSubscriptionAuthorizer($config->secret);
        $this->loop = $loop ?? Loop::get();
        $this->metrics = new Metrics;
        $this->hub = new Hub($this->metrics, $lifecycle);

        $this->metrics->gauge('murmur_connections', 'Open SSE connections.', $this->hub->connectionCount(...));
        $this->metrics->gauge('murmur_sockets', 'Accepted TCP sockets currently open.', fn (): int => count($this->clients));
        $this->metrics->gauge('murmur_topics', 'Topics with at least one subscriber.', $this->hub->topicCount(...));
        $this->metrics->gauge('murmur_uptime_seconds', 'Seconds since the server started listening.', fn (): float => $this->startedAt === null ? 0.0 : (hrtime(true) - $this->startedAt) / 1_000_000_000);
        $this->metrics->gauge('murmur_memory_bytes', 'Bytes of memory allocated by PHP.', fn (): int => memory_get_usage(true));
        $this->metrics->gauge('murmur_memory_peak_bytes', 'Peak bytes of memory allocated by PHP.', fn (): int => memory_get_peak_usage(true));
        $this->metrics->gauge('murmur_event_loop_lag_seconds', 'Largest event loop timer delay in seconds during the last second.', fn (): float => $this->loopLag);
    }

    /**
     * Get this server's metrics registry.
     */
    public function metrics(): Metrics
    {
        return $this->metrics;
    }

    /**
     * Start listening for HTTP requests once the live event log is ready.
     *
     * @return PromiseInterface<null>
     */
    public function listen(): PromiseInterface
    {
        if ($this->closed || $this->shutdown !== null) {
            return reject(new LogicException('The server has stopped accepting connections.'));
        }

        if ($this->listening === null) {
            $this->log->onGap($this->hub->gap(...));
            $this->log->subscribe($this->hub->dispatch(...));
            $this->listening = $this->log->ready()->then(function (): void {
                if ($this->closed) {
                    throw new LogicException('The server closed before it started listening.');
                }

                $this->bind();
            });
        }

        return $this->listening;
    }

    /**
     * Bind the HTTP routes and start sending periodic heartbeats.
     */
    protected function bind(): void
    {
        $sse = new SseHandler(
            $this->config,
            $this->log,
            $this->hub,
            $this->loop,
            $this->authorizer,
            fn (): bool => $this->shutdown === null,
            $this->metrics,
        );
        $publish = $this->config->publishEnabled && $this->log instanceof MemoryEventLog
            ? new PublishHandler($this->config, $this->log)
            : null;

        $http = new HttpServer($this->loop, new StreamingRequestMiddleware, function (ServerRequestInterface $request) use ($sse, $publish): ResponseInterface|PromiseInterface {
            $route = $request->getMethod().' '.$request->getUri()->getPath();
            $client = $this->clientFor($request);

            try {
                $response = match ($route) {
                    'POST /publish' => $publish === null ? new Response(404) : $publish($request),
                    'GET /sse' => $this->shutdown === null ? $sse($request, $client) : new Response(503),
                    'GET /ready' => $this->readinessResponse(),
                    'GET /health' => new Response(200, ['Content-Type' => 'application/json'], json_encode([
                        'status' => 'ok',
                        'connections' => $this->hub->connectionCount(),
                    ], JSON_THROW_ON_ERROR)),
                    default => new Response(404),
                };
            } catch (Throwable $error) {
                $response = $this->errorResponse($error);
            }

            $finish = fn (ResponseInterface $response): ResponseInterface => $this->finishResponse($response, $route, $client);

            return $response instanceof PromiseInterface
                ? $response->then($finish, fn (Throwable $error): ResponseInterface => $finish($this->errorResponse($error)))
                : $finish($response);
        });

        $this->socket = new SocketServer($this->config->host.':'.$this->config->port, [], $this->loop);
        $this->socket->on('connection', function (ConnectionInterface $client): void {
            if (count($this->clients) >= $this->config->maxSockets) {
                $client->close();

                return;
            }

            $key = $client->getLocalAddress().' '.$client->getRemoteAddress();
            $id = spl_object_id($client);
            $this->clients[$key] = $client;
            $this->requestDeadlines[$id] = $this->loop->addTimer(
                $this->config->requestTimeoutSeconds,
                function (TimerInterface $timer) use ($client, $id): void {
                    if (($this->requestDeadlines[$id] ?? null) === $timer) {
                        $client->close();
                    }
                },
            );
            $client->once('close', function () use ($key, $client): void {
                $this->cancelRequestDeadline($client);

                if (($this->clients[$key] ?? null) === $client) {
                    unset($this->clients[$key]);
                }

                if ($this->shutdown !== null && $this->clients === []) {
                    $this->close();
                }
            });
        });

        $http->listen($this->socket);
        $this->startedAt = hrtime(true);
        $this->errorCounter = fn () => $this->metrics->increment('murmur_errors_total');
        ErrorReporter::countUsing($this->errorCounter);

        if ($this->logger !== null) {
            ErrorReporter::logUsing($this->logger);
        }

        $this->heartbeat = new PeriodicTimer($this->loop, $this->config->heartbeatSeconds, $this->hub->heartbeat(...));
        $previousTick = hrtime(true);
        $ticks = 0;
        $worst = 0.0;
        $this->lagTimer = new PeriodicTimer($this->loop, static::LAG_INTERVAL, function () use (&$previousTick, &$ticks, &$worst): void {
            $now = hrtime(true);
            $lag = max(0.0, ($now - $previousTick) / 1_000_000_000 - static::LAG_INTERVAL);
            $previousTick = $now;
            $worst = max($worst, $lag);

            if (++$ticks === static::LAG_WINDOW_TICKS) {
                $this->loopLag = $worst;
                $ticks = 0;
                $worst = 0.0;
            }

            if ($lag * 1000 > $this->config->loopLagThresholdMs) {
                $this->reportLoopLag($now, $lag);
            }
        });
        $this->logInfo('Murmur is listening on '.$this->address());
    }

    /**
     * Count a lag threshold breach and report it at most once per ten seconds.
     */
    protected function reportLoopLag(int|float $now, float $lag): void
    {
        $this->metrics->increment('murmur_event_loop_lag_exceeded_total');

        if ($this->lastLagWarningAt !== null && $now - $this->lastLagWarningAt < 10_000_000_000) {
            return;
        }

        $this->lastLagWarningAt = $now;
        $message = sprintf('Murmur event loop lag exceeded the threshold: %.2F ms.', $lag * 1000);

        if ($this->logger !== null) {
            try {
                $this->logger->warning($message);

                return;
            } catch (Throwable) {
            }
        }

        error_log($message);
    }

    /**
     * Write a lifecycle message without letting a logger failure interrupt the server.
     */
    protected function logInfo(string $message): void
    {
        try {
            $this->logger?->info($message);
        } catch (Throwable) {
            error_log($message);
        }
    }

    /**
     * Complete the request deadline and count the resulting HTTP status.
     */
    protected function finishResponse(ResponseInterface $response, string $route, ConnectionInterface $client): ResponseInterface
    {
        $this->cancelRequestDeadline($client);

        if (! $client->isWritable()) {
            return $response;
        }

        $status = $response->getStatusCode();

        if ($route === 'POST /publish') {
            $this->metrics->increment('murmur_publish_requests_total', labels: ['status' => (string) $status]);
        } elseif ($route === 'GET /sse' && $status >= 400) {
            $this->metrics->increment('murmur_connections_rejected_total', labels: ['status' => (string) $status]);
        }

        if (in_array($route, ['GET /sse', 'GET /health', 'GET /ready'], true)) {
            $response = $response->withHeader('X-Murmur-Protocol', (string) static::PROTOCOL);
        }

        return $this->withResponseHeaders($response);
    }

    /**
     * Report whether this server can accept subscriptions and deliver live events.
     */
    protected function readinessResponse(): ResponseInterface
    {
        $ready = $this->socket !== null && $this->shutdown === null && $this->log->healthy();

        return new Response($ready ? 200 : 503, [
            'Content-Type' => 'application/json',
            'Cache-Control' => 'no-store',
        ], $ready ? '{"status":"ready"}' : '{"status":"unavailable"}');
    }

    /**
     * Report a route failure and respond with an internal server error.
     */
    protected function errorResponse(Throwable $error): ResponseInterface
    {
        ErrorReporter::report($error);

        return new Response(500);
    }

    /**
     * Close HTTP connections after one response and vary listed origins.
     */
    protected function withResponseHeaders(ResponseInterface $response): ResponseInterface
    {
        $response = $response->withHeader('Connection', 'close');

        if (! in_array('*', $this->config->allowedOrigins, true)) {
            $response = $response->withAddedHeader('Vary', 'Origin');
        }

        return $response;
    }

    /**
     * Cancel the deadline after a request completes or its socket closes.
     */
    protected function cancelRequestDeadline(ConnectionInterface $client): void
    {
        $id = spl_object_id($client);

        if (isset($this->requestDeadlines[$id])) {
            $this->loop->cancelTimer($this->requestDeadlines[$id]);
            unset($this->requestDeadlines[$id]);
        }
    }

    /**
     * Find the accepted socket using the endpoints recorded by the HTTP parser.
     *
     * @throws LogicException
     */
    protected function clientFor(ServerRequestInterface $request): ConnectionInterface
    {
        $params = $request->getServerParams();
        $local = 'tcp://'.($params['SERVER_ADDR'] ?? '').':'.($params['SERVER_PORT'] ?? '');
        $remote = 'tcp://'.($params['REMOTE_ADDR'] ?? '').':'.($params['REMOTE_PORT'] ?? '');

        return $this->clients[$local.' '.$remote] ?? throw new LogicException('No TCP socket matches the HTTP request.');
    }

    /**
     * Get the HTTP address of the listening socket.
     *
     * @throws LogicException
     */
    public function address(): string
    {
        $address = $this->socket?->getAddress();

        if ($address === null) {
            throw new LogicException('The server is not listening.');
        }

        return 'http://'.substr($address, strlen('tcp://'));
    }

    /**
     * Drain streams and in-flight requests before closing the remaining sockets.
     *
     * @return PromiseInterface<null>
     */
    public function shutdown(?float $graceSeconds = null): PromiseInterface
    {
        if ($this->shutdown !== null) {
            return $this->shutdown->promise();
        }

        if ($this->closed) {
            return resolve(null);
        }

        $this->shutdown = new Deferred;
        $this->logInfo('Murmur graceful shutdown started.');
        $this->socket?->close();
        $this->socket = null;

        if ($this->clients === []) {
            $this->close();
        } else {
            $grace = $graceSeconds ?? $this->config->shutdownGraceSeconds;
            $this->shutdownDeadline = $this->loop->addTimer($grace, $this->close(...));
            $this->drainStreams(min($this->config->shutdownSpreadSeconds, max(0.0, $grace) * 0.8));
        }

        return $this->shutdown->promise();
    }

    /**
     * End streams at an even pace over the spread, in connection order.
     *
     * A timer that fires early is rearmed, and the last stream ends no later than
     * the end of the spread.
     */
    protected function drainStreams(float $spreadSeconds): void
    {
        $connections = $this->hub->connections();
        $last = count($connections) - 1;

        if ($last < 1 || $spreadSeconds <= 0.0) {
            array_map($this->endStream(...), $connections);

            return;
        }

        $this->endStream($connections[0]);
        $ended = 1;
        $startedAt = hrtime(true);
        $interval = max($spreadSeconds / $last, static::DRAIN_INTERVAL);
        $nextAt = min($spreadSeconds, $interval);
        $this->streamEndTimer = $this->loop->addTimer($nextAt, function (TimerInterface $timer) use ($connections, $last, $spreadSeconds, $startedAt, $interval, &$ended, &$nextAt): void {
            $this->streamEndTimer = null;
            $elapsed = (hrtime(true) - $startedAt) / 1_000_000_000;
            $due = $elapsed >= $spreadSeconds ? $last + 1 : (int) floor($elapsed / $spreadSeconds * $last) + 1;

            while ($ended < $due) {
                $this->endStream($connections[$ended++]);
            }

            if ($ended <= $last && ! $this->closed) {
                if ($elapsed >= $nextAt) {
                    $nextAt = min($spreadSeconds, $elapsed + $interval);
                }

                $remaining = $nextAt - (hrtime(true) - $startedAt) / 1_000_000_000;
                $this->streamEndTimer = $this->loop->addTimer(max(0.001, $remaining), $timer->getCallback());
            }
        });
    }

    /**
     * Suggest a reconnect delay and flush the stream.
     */
    protected function endStream(Connection $connection): void
    {
        if (! $connection->isClosed()) {
            $min = $this->config->retryMs;
            $max = $min + min($min, PHP_INT_MAX - $min);
            $connection->end(Frame::retry($this->randomRetryMs($min, $max)));
        }
    }

    /**
     * Pick a uniform reconnect delay in milliseconds.
     */
    protected function randomRetryMs(int $min, int $max): int
    {
        return mt_rand($min, $max);
    }

    /**
     * Stop accepting clients, close connections, and release timers and the log.
     */
    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;
        $this->socket?->close();
        $this->socket = null;
        $this->hub->closeAll();

        foreach ($this->clients as $client) {
            $client->close();
        }

        $this->heartbeat?->cancel();
        $this->heartbeat = null;
        $this->lagTimer?->cancel();
        $this->lagTimer = null;

        if ($this->shutdownDeadline !== null) {
            $this->loop->cancelTimer($this->shutdownDeadline);
            $this->shutdownDeadline = null;
        }

        if ($this->streamEndTimer !== null) {
            $this->loop->cancelTimer($this->streamEndTimer);
            $this->streamEndTimer = null;
        }

        try {
            $this->log->close();
        } finally {
            ErrorReporter::forget($this->errorCounter, $this->logger);
            $this->errorCounter = null;
        }

        if ($this->shutdown !== null) {
            $this->logInfo('Murmur graceful shutdown finished.');
            $this->shutdown->resolve(null);
        }
    }
}
