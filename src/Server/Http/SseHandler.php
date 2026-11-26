<?php

namespace Arttecs\Murmur\Server\Http;

use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\Exceptions\InvalidToken;
use Arttecs\Murmur\Server\Auth\Grant;
use Arttecs\Murmur\Server\Auth\TokenVerifier;
use Arttecs\Murmur\Server\ConnectionId;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Hub;
use Arttecs\Murmur\Server\Metrics;
use Arttecs\Murmur\Server\Replay;
use Arttecs\Murmur\Server\ServerConfig;
use Arttecs\Murmur\Server\Sse\Frame;
use Arttecs\Murmur\Server\Topic;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use React\EventLoop\LoopInterface;
use React\Http\Message\Response;
use React\Promise\PromiseInterface;
use React\Socket\ConnectionInterface;
use React\Stream\ThroughStream;
use Throwable;

use function React\Promise\resolve;

/**
 * Authorize and stream an SSE subscription.
 *
 * @internal
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
        protected ?Metrics $metrics = null,
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
        } elseif ($request->hasHeader('Origin')) {
            if (! in_array(Origin::normalize($origin), $this->config->allowedOrigins, true)) {
                return new Response(403);
            }

            $headers['Access-Control-Allow-Origin'] = $origin;
        }

        $query = $request->getQueryParams();
        $topics = is_string($query['topics'] ?? null) ? explode(',', $query['topics']) : [];

        if ($topics === [] || count($topics) > $this->config->maxTopicsPerConnection || array_filter($topics, Topic::isValid(...)) !== $topics) {
            return new Response(400, $headers);
        }

        $id = $query['connection'] ?? null;

        if ($id !== null && (! is_string($id) || ! ConnectionId::isValid($id))) {
            return new Response(400, $headers);
        }

        $fresh = isset($query['fresh']) && is_string($query['fresh']) ? explode(',', $query['fresh']) : [];

        if (isset($query['fresh']) && ($fresh === [] || array_diff($fresh, $topics) !== [])) {
            return new Response(400, $headers);
        }

        $tokens = isset($query['token']) && is_string($query['token']) ? explode(',', $query['token']) : [];

        if (isset($query['token']) && ($tokens === [] || count($tokens) > $this->config->maxTopicsPerConnection || in_array('', $tokens, true))) {
            return new Response(400, $headers);
        }

        $privateTopics = array_filter($topics, Topic::isPrivate(...));

        if ($tokens === [] && $privateTopics !== []) {
            return new Response(401, $headers);
        }

        try {
            $grants = array_map($this->tokens->verify(...), $tokens);
        } catch (InvalidToken) {
            return new Response(401, $headers);
        }

        $expiresAt = $this->authorizationExpiry($privateTopics, $grants);

        if ($expiresAt === false) {
            return new Response(403, $headers);
        }

        if ($this->hub->connectionCount() >= $this->config->maxConnections) {
            return new Response(503, $headers);
        }

        $cursor = EventId::tryParse($request->getHeaderLine('Last-Event-ID'));

        if ($cursor === null && is_string($query['lastEventId'] ?? null)) {
            $cursor = EventId::tryParse($query['lastEventId']);
        }

        if ($cursor !== null) {
            $this->metrics?->increment('murmur_replays_total');
        }

        $stream = new ThroughStream;

        $connection = new HeldConnection($stream, $this->config->maxBufferBytes, $cursor, $this->metrics, $id, $fresh);

        $this->hub->attach($connection, $topics);
        $connection->onClose(function () use ($client): void {
            if ($client->isWritable()) {
                $client->close();
            }
        });

        $endStream = fn () => $this->endStream($stream, $client);

        if ($expiresAt !== null) {
            $timer = $this->loop->addTimer(max(0.0, $expiresAt - microtime(true)), $endStream);
            $connection->onClose(fn () => $this->loop->cancelTimer($timer));
        }

        $replayTopics = $cursor === null ? [] : array_values(array_diff($topics, $fresh));
        $snapshot = $fresh === [] ? null : $this->replay($connection, $cursor, $replayTopics);

        $this->loop->futureTick(function () use ($connection, $cursor, $replayTopics, $snapshot, $endStream): void {
            if ($connection->isClosed()) {
                return;
            }

            $connection->sendImmediately(Frame::retry($this->config->retryMs));

            if ($connection->isClosed()) {
                return;
            }

            $connection->sendImmediately(Frame::connected($connection->id));

            if ($connection->isClosed()) {
                return;
            }

            if ($cursor === null && $snapshot === null) {
                $connection->release();

                return;
            }

            ($snapshot ?? $this->replay($connection, $cursor, $replayTopics))->then(function (?Replay $replay) use ($connection, $endStream): void {
                if ($replay === null) {
                    $endStream();

                    return;
                }

                $connection->release($replay);
            })->catch($endStream);
        });

        return new Response(200, $headers + [
            'Content-Type' => 'text/event-stream',
            'Cache-Control' => 'no-cache',
            'X-Accel-Buffering' => 'no',
            'Connection' => 'close',
        ], $stream);
    }

    /**
     * Find when one of the requested private topics will no longer be covered.
     *
     * @param  array<int, string>  $topics
     * @param  list<Grant>  $grants
     * @return int|false|null Unix expiry, false for missing coverage, or null for public-only subscriptions.
     */
    protected function authorizationExpiry(array $topics, array $grants): int|false|null
    {
        $expiresAt = null;

        foreach ($topics as $topic) {
            $topicExpiry = null;

            foreach ($grants as $grant) {
                if ($grant->allows($topic)) {
                    $topicExpiry = max($topicExpiry ?? $grant->expiresAt, $grant->expiresAt);
                }
            }

            if ($topicExpiry === null) {
                return false;
            }

            $expiresAt = min($expiresAt ?? $topicExpiry, $topicExpiry);
        }

        return $expiresAt;
    }

    /**
     * Snapshot the log tail and request replay for the selected topics.
     *
     * @param  list<string>  $topics
     * @return PromiseInterface<Replay|null>
     */
    protected function replay(HeldConnection $connection, ?EventId $cursor, array $topics): PromiseInterface
    {
        $replay = resolve(null)->then(fn () => $this->log->since(
            $cursor ?? new EventId(0, 0),
            $topics,
            $this->config->maxReplayEvents,
            fn (Event $event): bool => $event->except !== $connection->id,
        ))->catch(fn (Throwable $error) => null);
        $connection->onClose($replay->cancel(...));

        return $replay;
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
