<?php

namespace Arttecs\Murmur\Server\Http;

use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\Contracts\SubscriptionAuthorizer;
use Arttecs\Murmur\Exceptions\InvalidToken;
use Arttecs\Murmur\Server\Auth\Grant;
use Arttecs\Murmur\Server\Connection;
use Arttecs\Murmur\Server\ConnectionId;
use Arttecs\Murmur\Server\ErrorReporter;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Hub;
use Arttecs\Murmur\Server\Metrics;
use Arttecs\Murmur\Server\Replay;
use Arttecs\Murmur\Server\ServerConfig;
use Arttecs\Murmur\Server\Sse\Frame;
use Arttecs\Murmur\Server\Topic;
use Closure;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use React\EventLoop\LoopInterface;
use React\EventLoop\TimerInterface;
use React\Http\Message\Response;
use React\Promise\Deferred;
use React\Promise\PromiseInterface;
use React\Socket\ConnectionInterface;
use React\Stream\ThroughStream;
use Throwable;

use function React\Promise\all;
use function React\Promise\resolve;

/**
 * Authorize and stream an SSE subscription.
 *
 * @internal
 */
class SseHandler
{
    /**
     * The longest grant expiry delay to schedule, in seconds (ten years).
     */
    protected const MAX_EXPIRY_DELAY_SECONDS = 315_360_000;

    /**
     * Create an SSE handler that shares the server's hub, log, and loop.
     *
     * @param  Closure(): bool  $accepting
     */
    public function __construct(
        protected ServerConfig $config,
        protected EventLog $log,
        protected Hub $hub,
        protected LoopInterface $loop,
        protected SubscriptionAuthorizer $authorizer,
        protected Closure $accepting,
        protected ?Metrics $metrics = null,
    ) {}

    /**
     * Validate a subscription and wait for the authorizer's decision.
     *
     * @return ResponseInterface|PromiseInterface<ResponseInterface>
     */
    public function __invoke(ServerRequestInterface $request, ConnectionInterface $client): ResponseInterface|PromiseInterface
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

        $cursor = EventId::tryParse($request->getHeaderLine('Last-Event-ID'));

        if ($cursor === null && is_string($query['lastEventId'] ?? null)) {
            $cursor = EventId::tryParse($query['lastEventId']);
        }

        $authorization = all(array_map(
            fn (?string $token): PromiseInterface => resolve(null)->then(fn () => $this->authorizer->authorize($token, $topics)),
            $tokens === [] ? [null] : array_values(array_unique($tokens)),
        ));

        $decision = new Deferred(function (callable $resolve) use ($authorization): void {
            $resolve(null);
            $authorization->cancel();
        });
        $authorization->then($decision->resolve(...), $decision->reject(...));

        return $decision->promise()->then(function (?array $grants) use ($client, $headers, $topics, $fresh, $id, $cursor): ResponseInterface {
            if ($grants === null || ! $client->isWritable() || ! ($this->accepting)()) {
                return new Response(503, $headers);
            }

            $now = time();

            if (array_filter($grants, fn (Grant $grant): bool => $grant->expiresAt <= $now) !== []) {
                return new Response(401, $headers);
            }

            $expiresAt = $this->authorizationExpiry(array_filter($topics, Topic::isPrivate(...)), $grants);

            if ($expiresAt === false) {
                return new Response(403, $headers);
            }

            if ($this->hub->connectionCount() >= $this->config->maxConnections) {
                return new Response(503, $headers);
            }

            return $this->subscribe($client, $headers, $topics, $fresh, $id, $cursor, $expiresAt);
        })->catch(function (Throwable $error) use ($headers): ResponseInterface {
            if ($error instanceof InvalidToken) {
                return new Response(401, $headers);
            }

            ErrorReporter::report($error);

            return new Response(500, $headers);
        });
    }

    /**
     * Attach the authorized connection and schedule initial frames after the body is piped.
     *
     * @param  array<string, string>  $headers
     * @param  list<string>  $topics
     * @param  list<string>  $fresh
     */
    protected function subscribe(
        ConnectionInterface $client,
        array $headers,
        array $topics,
        array $fresh,
        ?string $id,
        ?EventId $cursor,
        ?int $expiresAt,
    ): ResponseInterface {
        if ($cursor !== null) {
            $this->metrics?->increment('murmur_replays_total');
        }

        $stream = new ThroughStream;

        $connection = new HeldConnection($stream, $this->config->maxBufferBytes, $cursor, $this->metrics, $id);

        $this->hub->attach($connection, $topics);
        $connection->onClose(function () use ($client): void {
            if ($client->isWritable()) {
                $client->close();
            }
        });

        $endStream = fn (string $reason) => $this->endStream($connection, $client, $reason);

        $delay = $expiresAt === null ? null : $expiresAt - microtime(true);

        if ($delay !== null && $delay <= static::MAX_EXPIRY_DELAY_SECONDS && $delay < intdiv(PHP_INT_MAX, 1000)) {
            $this->scheduleExpiry($connection, $client, $expiresAt, $delay);
        }

        $replayTopics = $cursor === null ? [] : array_values(array_diff($topics, $fresh));
        $snapshot = $this->replay($connection, $cursor, $replayTopics);

        $this->loop->futureTick(function () use ($connection, $snapshot, $endStream, $fresh): void {
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

            $snapshot->then(function (?Replay $replay) use ($connection, $endStream, $fresh): void {
                if ($replay === null) {
                    $endStream('error');

                    return;
                }

                $connection->release($replay, $fresh);
            })->catch(function (Throwable $error) use ($endStream): void {
                $this->reportReplayFailure($error);
                $endStream('error');
            });
        });

        return new Response(200, $headers + [
            'Content-Type' => 'text/event-stream',
            'Cache-Control' => 'no-cache',
            'X-Accel-Buffering' => 'no',
            'Connection' => 'close',
        ], $stream);
    }

    /**
     * End the stream as expired once the grant's deadline has passed on the wall clock.
     *
     * A timer that fires early is rearmed for the remaining time. Closing the
     * connection cancels the pending timer.
     */
    protected function scheduleExpiry(Connection $connection, ConnectionInterface $client, int $expiresAt, float $delay): void
    {
        $timer = $this->loop->addTimer(max(0.0, $delay), function (TimerInterface $fired) use ($connection, $client, $expiresAt, &$timer): void {
            $remaining = $expiresAt - microtime(true);

            if ($remaining > 0.0) {
                $timer = $this->loop->addTimer(max(0.001, $remaining), $fired->getCallback());

                return;
            }

            $timer = null;
            $this->endStream($connection, $client, 'expired');
        });

        $connection->onClose(function () use (&$timer): void {
            if ($timer !== null) {
                $this->loop->cancelTimer($timer);
                $timer = null;
            }
        });
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
        ))->catch(function (Throwable $error) use ($connection): ?Replay {
            if (! $connection->isClosed()) {
                $this->reportReplayFailure($error);
            }

            return null;
        });
        $connection->onClose($replay->cancel(...));

        return $replay;
    }

    /**
     * Count and report a failure while preparing or releasing a subscription.
     */
    protected function reportReplayFailure(Throwable $error): void
    {
        $this->metrics?->increment('murmur_replay_failures_total');
        ErrorReporter::report($error);
    }

    /**
     * End the response and give the socket one second to flush before closing it.
     */
    protected function endStream(Connection $connection, ConnectionInterface $client, string $reason): void
    {
        if ($connection->isClosed()) {
            return;
        }

        $timer = $this->loop->addTimer(1.0, $client->close(...));
        $client->once('close', fn () => $this->loop->cancelTimer($timer));
        $connection->end(reason: $reason);
    }
}
