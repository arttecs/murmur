<?php

namespace Arttecs\Murmur\Server;

use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\Server\Http\MetricsHandler;
use Arttecs\Murmur\Server\Http\PublishHandler;
use Arttecs\Murmur\Server\Http\SseHandler;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
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
     * The loop used for HTTP and connection timers.
     */
    protected LoopInterface $loop;

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
    protected ?TimerInterface $heartbeat = null;

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
     * Create a server using the given configuration, event log, loop, and logger.
     */
    public function __construct(
        protected ServerConfig $config,
        protected EventLog $log,
        ?LoopInterface $loop = null,
        protected ?LoggerInterface $logger = null,
    ) {
        $this->loop = $loop ?? Loop::get();
        $this->metrics = new Metrics;
        $this->hub = new Hub($this->metrics);

        $this->metrics->gauge('murmur_connections', 'Open SSE connections.', $this->hub->connectionCount(...));
        $this->metrics->gauge('murmur_sockets', 'Accepted TCP sockets currently open.', fn (): int => count($this->clients));
        $this->metrics->gauge('murmur_topics', 'Topics with at least one subscriber.', $this->hub->topicCount(...));
        $this->metrics->gauge('murmur_uptime_seconds', 'Seconds since the server started listening.', fn (): float => $this->startedAt === null ? 0.0 : (hrtime(true) - $this->startedAt) / 1_000_000_000);
        $this->metrics->gauge('murmur_memory_bytes', 'Bytes of memory allocated by PHP.', fn (): int => memory_get_usage(true));
        $this->metrics->gauge('murmur_memory_peak_bytes', 'Peak bytes of memory allocated by PHP.', fn (): int => memory_get_peak_usage(true));
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
        $sse = new SseHandler($this->config, $this->log, $this->hub, $this->loop, $this->metrics);
        $metrics = new MetricsHandler($this->config, $this->metrics);
        $publish = $this->config->publishEnabled && $this->log instanceof MemoryEventLog
            ? new PublishHandler($this->config, $this->log)
            : null;

        $http = new HttpServer($this->loop, new StreamingRequestMiddleware, function (ServerRequestInterface $request) use ($sse, $metrics, $publish): ResponseInterface|PromiseInterface {
            $route = $request->getMethod().' '.$request->getUri()->getPath();
            $client = $this->clientFor($request);

            try {
                $response = match ($route) {
                    'POST /publish' => $publish === null ? new Response(404) : $publish($request),
                    'GET /sse' => $this->shutdown === null ? $sse($request, $client) : new Response(503),
                    'GET /metrics' => $metrics($request),
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
            $this->clients[$key] = $client;
            $this->requestDeadlines[spl_object_id($client)] = $this->loop->addTimer(
                $this->config->requestTimeoutSeconds,
                $client->close(...),
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

        $this->heartbeat = $this->loop->addPeriodicTimer($this->config->heartbeatSeconds, $this->hub->heartbeat(...));
        $this->logInfo('Murmur is listening on '.$this->address());
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
        $status = $response->getStatusCode();

        if ($route === 'POST /publish') {
            $this->metrics->increment('murmur_publish_requests_total', labels: ['status' => (string) $status]);
        } elseif ($route === 'GET /sse' && $status >= 400) {
            $this->metrics->increment('murmur_connections_rejected_total', labels: ['status' => (string) $status]);
        }

        return $this->withResponseHeaders($response);
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
            $this->shutdownDeadline = $this->loop->addTimer($graceSeconds ?? $this->config->shutdownGraceSeconds, $this->close(...));
            $this->hub->endAll();
        }

        return $this->shutdown->promise();
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

        if ($this->heartbeat !== null) {
            $this->loop->cancelTimer($this->heartbeat);
            $this->heartbeat = null;
        }

        if ($this->shutdownDeadline !== null) {
            $this->loop->cancelTimer($this->shutdownDeadline);
            $this->shutdownDeadline = null;
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
