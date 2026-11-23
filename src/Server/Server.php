<?php

namespace Arttecs\Murmur\Server;

use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\Server\Http\PublishHandler;
use Arttecs\Murmur\Server\Http\SseHandler;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use LogicException;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
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
     * Indicates whether the server has shut down.
     */
    protected bool $closed = false;

    /**
     * Create a server using the given configuration, log, and loop.
     */
    public function __construct(protected ServerConfig $config, protected EventLog $log, ?LoopInterface $loop = null)
    {
        $this->loop = $loop ?? Loop::get();
        $this->hub = new Hub;
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
        $sse = new SseHandler($this->config, $this->log, $this->hub, $this->loop);
        $publish = $this->config->publishEnabled && $this->log instanceof MemoryEventLog
            ? new PublishHandler($this->config, $this->log)
            : null;

        $http = new HttpServer($this->loop, new StreamingRequestMiddleware, function (ServerRequestInterface $request) use ($sse, $publish): ResponseInterface|PromiseInterface {
            $route = $request->getMethod().' '.$request->getUri()->getPath();

            $client = $this->clientFor($request);

            if ($route === 'POST /publish' && $publish !== null) {
                return resolve($publish($request))->then(function (ResponseInterface $response) use ($client): ResponseInterface {
                    $this->cancelRequestDeadline($client);

                    return $this->withResponseHeaders($response);
                });
            }

            $this->cancelRequestDeadline($client);

            try {
                $response = match ($route) {
                    'GET /sse' => $this->shutdown === null ? $sse($request, $client) : new Response(503),
                    'GET /health' => new Response(200, ['Content-Type' => 'application/json'], json_encode([
                        'status' => 'ok',
                        'connections' => $this->hub->connectionCount(),
                    ], JSON_THROW_ON_ERROR)),
                    default => new Response(404),
                };
            } catch (Throwable $error) {
                ErrorReporter::report($error);

                $response = new Response(500);
            }

            return $this->withResponseHeaders($response);
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
        $this->heartbeat = $this->loop->addPeriodicTimer($this->config->heartbeatSeconds, $this->hub->heartbeat(...));
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
    public function shutdown(float $graceSeconds = 5.0): PromiseInterface
    {
        if ($this->shutdown !== null) {
            return $this->shutdown->promise();
        }

        if ($this->closed) {
            return resolve(null);
        }

        $this->shutdown = new Deferred;
        $this->socket?->close();
        $this->socket = null;

        if ($this->clients === []) {
            $this->close();
        } else {
            $this->shutdownDeadline = $this->loop->addTimer($graceSeconds, $this->close(...));
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

        $this->log->close();
        $this->shutdown?->resolve(null);
    }
}
