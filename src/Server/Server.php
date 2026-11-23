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
use React\Promise\PromiseInterface;
use React\Socket\ConnectionInterface;
use React\Socket\SocketServer;

/**
 * Serve Murmur's HTTP routes and manage the lifetime of its connections.
 */
class Server
{
    /**
     * The loop used for HTTP and connection timers.
     */
    private LoopInterface $loop;

    /**
     * The hub shared by subscriptions and the event log.
     */
    private Hub $hub;

    /**
     * The listening socket.
     */
    private ?SocketServer $socket = null;

    /**
     * The accepted HTTP sockets that have yet to close.
     *
     * @var array<int, ConnectionInterface>
     */
    private array $clients = [];

    /**
     * The timer that sends periodic heartbeats.
     */
    private ?TimerInterface $heartbeat = null;

    /**
     * Indicates whether the server has shut down.
     */
    private bool $closed = false;

    /**
     * Create a server using the given configuration, log, and loop.
     */
    public function __construct(private ServerConfig $config, private EventLog $log, ?LoopInterface $loop = null)
    {
        $this->loop = $loop ?? Loop::get();
        $this->hub = new Hub;
    }

    /**
     * Start listening for HTTP requests and dispatching live events.
     */
    public function listen(): void
    {
        if ($this->closed || $this->socket !== null) {
            return;
        }

        $sse = new SseHandler($this->config, $this->log, $this->hub, $this->loop);
        $publish = $this->config->publishEnabled && $this->log instanceof MemoryEventLog
            ? new PublishHandler($this->config, $this->log)
            : null;

        $http = new HttpServer($this->loop, new StreamingRequestMiddleware, function (ServerRequestInterface $request) use ($sse, $publish): ResponseInterface|PromiseInterface {
            $route = $request->getMethod().' '.$request->getUri()->getPath();

            if ($route === 'POST /publish' && $publish !== null) {
                return $publish($request);
            }

            $response = match ($route) {
                'GET /sse' => $sse($request),
                'GET /health' => Response::json(['status' => 'ok', 'connections' => $this->hub->connectionCount()]),
                default => new Response(404),
            };

            return $request->getBody()->getSize() === 0
                ? $response
                : $response->withHeader('Connection', 'close');
        });

        $host = $this->config->host;

        if (str_contains($host, ':') && ! str_starts_with($host, '[')) {
            $host = '['.$host.']';
        }

        $this->socket = new SocketServer($host.':'.$this->config->port, [], $this->loop);
        $this->socket->on('connection', function (ConnectionInterface $client): void {
            $id = spl_object_id($client);
            $this->clients[$id] = $client;
            $client->on('close', function () use ($id): void {
                unset($this->clients[$id]);
            });
        });

        $http->listen($this->socket);
        $this->log->subscribe($this->hub->dispatch(...));
        $this->heartbeat = $this->loop->addPeriodicTimer($this->config->heartbeatSeconds, $this->hub->heartbeat(...));
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

        $this->log->close();
    }
}
