<?php

namespace Arttecs\Murmur\Server\Http;

use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\Exceptions\InvalidToken;
use Arttecs\Murmur\Server\Auth\TokenVerifier;
use Arttecs\Murmur\Server\Connection;
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
    private TokenVerifier $tokens;

    /**
     * Create an SSE handler that shares the server's hub, log, and loop.
     */
    public function __construct(
        private ServerConfig $config,
        private EventLog $log,
        private Hub $hub,
        private LoopInterface $loop,
    ) {
        $this->tokens = new TokenVerifier($config->secret);
    }

    /**
     * Validate a subscription and schedule its initial frames once the body is piped.
     */
    public function __invoke(ServerRequestInterface $request): ResponseInterface
    {
        $headers = [];
        $origin = $request->getHeaderLine('Origin');

        if (in_array('*', $this->config->allowedOrigins, true)) {
            $headers['Access-Control-Allow-Origin'] = '*';
        } elseif ($origin !== '' && in_array($origin, $this->config->allowedOrigins, true)) {
            $headers['Access-Control-Allow-Origin'] = $origin;
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

        $cursorValue = $request->hasHeader('Last-Event-ID') ? $request->getHeaderLine('Last-Event-ID') : ($query['lastEventId'] ?? null);
        $cursor = is_string($cursorValue) ? EventId::tryParse($cursorValue) : null;

        $stream = new ThroughStream;

        $connection = new
        /**
         * Hold frames from the hub until replay has caught up.
         */
        class($stream, $this->config->maxBufferBytes) extends Connection
        {
            /**
             * The live frames held until replay finishes, or null once delivery is live.
             *
             * @var list<string>|null
             */
            private ?array $heldFrames = [];

            /**
             * Create a held connection to the HTTP response stream.
             */
            public function __construct(private ThroughStream $output, int $maxBufferBytes)
            {
                parent::__construct($output, $maxBufferBytes);
            }

            /**
             * Hold incoming hub frames during replay and send them directly afterward.
             */
            public function send(string $frame): void
            {
                if ($this->heldFrames === null) {
                    parent::send($frame);
                } else {
                    $this->heldFrames[] = $frame;
                }
            }

            /**
             * Write a protocol or replay frame ahead of the held live frames.
             */
            public function sendImmediately(string $frame): void
            {
                parent::send($frame);
            }

            /**
             * Drain held frames, discarding event IDs already covered by replay.
             */
            public function release(?EventId $last): void
            {
                for ($index = 0; isset($this->heldFrames[$index]); $index++) {
                    $frame = $this->heldFrames[$index];

                    if ($last !== null && str_starts_with($frame, 'id: ')) {
                        $line = strstr($frame, "\n", true);
                        $id = EventId::tryParse(substr($line, 4));

                        if ($id !== null && $id->compare($last) <= 0) {
                            continue;
                        }
                    }

                    $this->sendImmediately($frame);
                }

                $this->heldFrames = null;
            }

            /**
             * End the streamed response and run the connection's cleanup listeners.
             */
            public function close(): void
            {
                $this->heldFrames = null;
                $this->output->end();

                parent::close();
            }
        };

        $this->hub->attach($connection, $topics);

        if ($grant !== null) {
            $timer = $this->loop->addTimer(max(0.0, $grant->expiresAt - microtime(true)), $connection->close(...));
            $connection->onClose(fn () => $this->loop->cancelTimer($timer));
        }

        $this->loop->futureTick(function () use ($connection, $cursor, $topics): void {
            if ($connection->isClosed()) {
                return;
            }

            $connection->sendImmediately(Frame::retry($this->config->retryMs));

            if ($connection->isClosed()) {
                return;
            }

            if ($cursor === null) {
                $connection->release(null);

                return;
            }

            try {
                $replay = $this->log->since($cursor, $topics)->then(function (Replay $replay) use ($connection, $cursor): void {
                    if ($replay->gap) {
                        $connection->sendImmediately(Frame::gap());
                    }

                    $last = $cursor;

                    foreach ($replay->events as $event) {
                        if ($connection->isClosed()) {
                            return;
                        }

                        $connection->sendImmediately(Frame::event($event));
                        $last = $event->id ?? $last;
                    }

                    $connection->release($last);
                });
            } catch (Throwable) {
                $connection->close();

                return;
            }

            $replay->catch($connection->close(...));
            $connection->onClose($replay->cancel(...));
        });

        return new Response(200, $headers + [
            'Content-Type' => 'text/event-stream',
            'Cache-Control' => 'no-cache',
            'X-Accel-Buffering' => 'no',
            'Connection' => 'close',
        ], $stream);
    }
}
