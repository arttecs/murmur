<?php

namespace Arttecs\Murmur\Server\Http;

use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\Exceptions\InvalidToken;
use Arttecs\Murmur\Server\Auth\TokenVerifier;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Hub;
use Arttecs\Murmur\Server\Replay;
use Arttecs\Murmur\Server\ServerConfig;
use Arttecs\Murmur\Server\Sse\Frame;
use Arttecs\Murmur\Server\Topic;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use React\EventLoop\LoopInterface;
use React\Http\Message\Response;
use React\Socket\ConnectionInterface;
use React\Stream\ThroughStream;
use Throwable;

/**
 * Authorize and stream an SSE subscription.
 */
class SseHandler
{
    /**
     * The verifier used to authorize subscription tokens.
     */
    protected TokenVerifier $tokens;

    /**
     * Create an SSE handler that shares the server's hub, log, and loop.
     */
    public function __construct(
        protected ServerConfig $config,
        protected EventLog $log,
        protected Hub $hub,
        protected LoopInterface $loop,
    ) {
        $this->tokens = new TokenVerifier($config->secret);
    }

    /**
     * Validate a subscription and schedule its initial frames once the body is piped.
     */
    public function __invoke(ServerRequestInterface $request, ConnectionInterface $client): ResponseInterface
    {
        $headers = [];
        $origin = $request->getHeaderLine('Origin');

        if (in_array('*', $this->config->allowedOrigins, true)) {
            $headers['Access-Control-Allow-Origin'] = '*';
        } elseif ($origin !== '' && in_array($origin, $this->config->allowedOrigins, true)) {
            $headers['Access-Control-Allow-Origin'] = $origin;
            $headers['Vary'] = 'Origin';
        }

        $query = $request->getQueryParams();
        $topics = is_string($query['topics'] ?? null) ? explode(',', $query['topics']) : [];

        if ($topics === [] || count($topics) > $this->config->maxTopicsPerConnection || array_filter($topics, Topic::isValid(...)) !== $topics) {
            return new Response(400, $headers);
        }

        $grant = null;

        if (is_string($query['token'] ?? null)) {
            try {
                $grant = $this->tokens->verify($query['token']);
            } catch (InvalidToken) {
                $grant = null;
            }
        }

        foreach (array_filter($topics, Topic::isPrivate(...)) as $topic) {
            if ($grant === null) {
                return new Response(401, $headers);
            }

            if (! $grant->allows($topic)) {
                return new Response(403, $headers);
            }
        }

        if ($this->hub->connectionCount() >= $this->config->maxConnections) {
            return new Response(503, $headers);
        }

        $cursor = EventId::tryParse($request->getHeaderLine('Last-Event-ID'));

        if ($cursor === null && is_string($query['lastEventId'] ?? null)) {
            $cursor = EventId::tryParse($query['lastEventId']);
        }

        $stream = new ThroughStream;

        $connection = new HeldConnection($stream, $this->config->maxBufferBytes, $cursor);

        $this->hub->attach($connection, $topics);
        $connection->onClose(function () use ($client): void {
            if ($client->isWritable()) {
                $client->close();
            }
        });

        $endStream = fn () => $this->endStream($stream, $client);

        if ($grant !== null) {
            $timer = $this->loop->addTimer(max(0.0, $grant->expiresAt - microtime(true)), $endStream);
            $connection->onClose(fn () => $this->loop->cancelTimer($timer));
        }

        $this->loop->futureTick(function () use ($connection, $cursor, $topics, $endStream): void {
            if ($connection->isClosed()) {
                return;
            }

            $connection->sendImmediately(Frame::retry($this->config->retryMs));

            if ($connection->isClosed()) {
                return;
            }

            if ($cursor === null) {
                $connection->release();

                return;
            }

            try {
                $replay = $this->log->since($cursor, $topics)->then(function (Replay $replay) use ($connection): void {
                    if ($replay->gap) {
                        $connection->sendImmediately(Frame::gap());
                    }

                    $connection->release($replay->events);
                });
            } catch (Throwable) {
                $endStream();

                return;
            }

            $replay->catch($endStream);
            $connection->onClose($replay->cancel(...));
        });

        return new Response(200, $headers + [
            'Content-Type' => 'text/event-stream',
            'Cache-Control' => 'no-cache',
            'X-Accel-Buffering' => 'no',
            'Connection' => 'close',
        ], $stream);
    }

    /**
     * End the response and give the socket one second to flush before closing it.
     */
    protected function endStream(ThroughStream $stream, ConnectionInterface $client): void
    {
        if (! $stream->isWritable()) {
            return;
        }

        $timer = $this->loop->addTimer(1.0, $client->close(...));
        $client->once('close', fn () => $this->loop->cancelTimer($timer));
        $stream->end();
    }
}
