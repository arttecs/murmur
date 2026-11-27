<?php

namespace Arttecs\Murmur\Server\Log;

use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\Server\ErrorReporter;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Replay;
use Clue\React\Redis\Client;
use Clue\React\Redis\Factory;
use Clue\Redis\Protocol\Model\ErrorReply;
use InvalidArgumentException;
use JsonException;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\EventLoop\TimerInterface;
use React\Promise\Deferred;
use React\Promise\PromiseInterface;
use React\Promise\Timer\TimeoutException;
use RuntimeException;
use Throwable;

use function React\Promise\reject;
use function React\Promise\resolve;
use function React\Promise\Timer\timeout;

/**
 * Replay and subscribe to events stored in a Redis stream.
 *
 * @internal
 */
class RedisEventLog implements EventLog
{
    /**
     * The most entries one live read may return, so a lagging reader is detected on every Redis version.
     */
    protected const READ_COUNT = 1000;

    /**
     * The connection reserved for blocking stream reads.
     */
    protected Client $reader;

    /**
     * The connection used for replay queries.
     */
    protected Client $replay;

    /**
     * The loop used for Redis connections and retry timers.
     */
    protected LoopInterface $loop;

    /**
     * The last stream identifier processed by the reader.
     */
    protected ?string $cursor = null;

    /**
     * The pending wait for the starting cursor.
     *
     * @var Deferred<null>|null
     */
    protected ?Deferred $readiness = null;

    /**
     * The deadline for the outstanding blocking read.
     */
    protected ?TimerInterface $readDeadline = null;

    /**
     * Indicates whether a reader failure is awaiting recovery.
     */
    protected bool $readerDisconnected = false;

    /**
     * Indicates whether the current reader outage has already triggered a gap.
     */
    protected bool $readerGapReported = false;

    /**
     * The pending timer for resuming stream reads after a Redis error.
     */
    protected ?TimerInterface $retry = null;

    /**
     * The listeners that receive newly read events.
     *
     * @var list<callable(Event): void>
     */
    protected array $listeners = [];

    /**
     * The listeners notified when the reader loses history.
     *
     * @var list<callable(): void>
     */
    protected array $gapListeners = [];

    /**
     * Indicates whether the log has been closed.
     */
    protected bool $closed = false;

    /**
     * Create an event log with independent Redis connections for reading and replay.
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        protected string $redisUrl,
        protected string $streamKey,
        protected int $maxLength = 10000,
        ?LoopInterface $loop = null,
        protected int $maxPayloadBytes = 65536,
        protected float $queryTimeoutSeconds = 5.0,
        protected int $blockMs = 5000,
    ) {
        if ($maxLength < 1) {
            throw new InvalidArgumentException('The event log capacity must be at least 1.');
        }

        if ($queryTimeoutSeconds <= 0 || ! is_finite($queryTimeoutSeconds)) {
            throw new InvalidArgumentException('The queryTimeoutSeconds setting must be positive and finite.');
        }

        if ($blockMs <= 0) {
            throw new InvalidArgumentException('The blockMs setting must be positive.');
        }

        $this->loop = $loop ?? Loop::get();
        $factory = new Factory($this->loop);
        $this->reader = $factory->createLazyClient($redisUrl);
        $this->replay = $factory->createLazyClient($redisUrl);
    }

    /**
     * Replay retained events after the given identifier for the requested topics.
     *
     * Read the tail before the replay range and check for trimming afterward.
     * A concurrent trim can flag a spurious gap but cannot hide missing events.
     *
     * @param  list<string>  $topics
     * @param  (callable(Event): bool)|null  $filter
     * @return PromiseInterface<Replay>
     */
    public function since(EventId $after, array $topics, int $limit, ?callable $filter = null): PromiseInterface
    {
        return $this->query($this->replay, 'xrevrange', $this->streamKey, '+', '-', 'COUNT', 1)->then(function (array $newest) use ($after, $topics, $limit, $filter): PromiseInterface {
            $last = $newest[0][0] ?? null;
            $tail = $last === null ? null : EventId::tryParse($last);

            if ($last === null || $topics === []) {
                return resolve(new Replay(false, [], $tail));
            }

            return $this->readReplayPage((string) $after, $last, $topics, $limit, $filter)->then(function (?array $events) use ($after, $tail): PromiseInterface|Replay {
                if ($events === null) {
                    return new Replay(true, [], $tail);
                }

                return $this->hasGap($after, $this->replay)->then(fn (bool $gap): Replay => new Replay($gap, $events, $tail));
            });
        });
    }

    /**
     * Read at most one page of Redis entries at a time and keep only matching events.
     *
     * @param  list<string>  $topics
     * @param  (callable(Event): bool)|null  $filter
     * @param  list<Event>  $events
     * @return PromiseInterface<list<Event>|null>
     */
    protected function readReplayPage(string $cursor, string $last, array $topics, int $limit, ?callable $filter, array $events = []): PromiseInterface
    {
        return $this->query($this->replay, 'xrange', $this->streamKey, '('.$cursor, $last, 'COUNT', 500)->then(function (array $entries) use ($last, $topics, $limit, $filter, $events): PromiseInterface|array|null {
            foreach ($entries as $entry) {
                $event = $this->decode($entry);

                if ($event !== null && in_array($event->topic, $topics, true) && ($filter === null || $filter($event))) {
                    if (count($events) === $limit) {
                        return null;
                    }

                    $events[] = $event;
                }
            }

            return count($entries) === 500
                ? $this->readReplayPage($entries[499][0], $last, $topics, $limit, $filter, $events)
                : $events;
        });
    }

    /**
     * Check whether Redis removed entries before the requested cursor.
     *
     * An unparseable reader cursor is beyond the supported range of identifiers.
     *
     * @return PromiseInterface<bool>
     */
    protected function hasGap(?EventId $after, Client $client, bool $allowMissing = true): PromiseInterface
    {
        return $this->query($client, 'xinfo', 'STREAM', $this->streamKey)->then(function (array $fields) use ($after): bool {
            $info = array_column(array_chunk($fields, 2), 1, 0);
            $firstEntry = $info['first-entry'];

            if ($firstEntry === null) {
                return false;
            }

            $first = EventId::tryParse($firstEntry[0]);

            return $first === null || ($after !== null && $info['entries-added'] > $info['length'] && $after->compare($first) < 0);
        }, function (Throwable $error) use ($allowMissing): bool {
            if ($allowMissing && $error instanceof ErrorReply && $error->getMessage() === 'ERR no such key') {
                return false;
            }

            throw $error;
        });
    }

    /**
     * Run a non-blocking Redis command with a deadline for its reply.
     *
     * @return PromiseInterface<array>
     */
    protected function query(Client $client, string $command, string|int ...$arguments): PromiseInterface
    {
        return timeout($client->{$command}(...$arguments), $this->queryTimeoutSeconds, $this->loop)
            ->catch(function (TimeoutException $error) use ($client): never {
                $this->disconnect($client);

                throw $error;
            });
    }

    /**
     * Replace a timed-out lazy client and close its outstanding operations.
     */
    protected function disconnect(Client $client): void
    {
        if ($client === $this->reader) {
            $this->reader = (new Factory($this->loop))->createLazyClient($this->redisUrl);
        } elseif ($client === $this->replay) {
            $this->replay = (new Factory($this->loop))->createLazyClient($this->redisUrl);
        }

        $client->close();
    }

    /**
     * Register a listener and start reading the stream on the first subscription.
     *
     * @param  callable(Event): void  $listener
     */
    public function subscribe(callable $listener): void
    {
        if ($this->closed) {
            return;
        }

        $this->listeners[] = $listener;

        if (count($this->listeners) === 1) {
            $this->read();
        }
    }

    /**
     * Register a listener for history lost during live delivery.
     *
     * @param  callable(): void  $listener
     */
    public function onGap(callable $listener): void
    {
        if (! $this->closed) {
            $this->gapListeners[] = $listener;
        }
    }

    /**
     * Wait until the live reader has established its starting cursor.
     *
     * @return PromiseInterface<null>
     */
    public function ready(): PromiseInterface
    {
        if ($this->closed) {
            return reject(new RuntimeException('The Redis event log is closed.'));
        }

        if ($this->cursor !== null) {
            return resolve(null);
        }

        return ($this->readiness ??= new Deferred)->promise();
    }

    /**
     * Report whether the live reader has started and is outside a recovery cycle.
     */
    public function healthy(): bool
    {
        return ! $this->closed && $this->cursor !== null && ! $this->readerDisconnected;
    }

    /**
     * Stop reading and close both Redis connections.
     */
    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;
        $this->listeners = [];
        $this->gapListeners = [];

        if ($this->retry !== null) {
            $this->loop->cancelTimer($this->retry);
            $this->retry = null;
        }

        $this->cancelReadDeadline();
        $this->reader->close();
        $this->replay->close();
        $this->readiness?->reject(new RuntimeException('The Redis event log closed before it was ready.'));
        $this->readiness = null;
    }

    /**
     * Resolve the starting cursor or read new stream entries from the current cursor.
     *
     * If MAXLEN trimming passed the cursor, every retained entry is newer
     * than the cursor. The reply may still be capped by the blocked-read limit.
     */
    protected function read(): void
    {
        if ($this->closed) {
            return;
        }

        if ($this->cursor === null) {
            $this->query($this->reader, 'xrevrange', $this->streamKey, '+', '-', 'COUNT', 1)->then(function (array $entries): void {
                $this->cursor = $entries[0][0] ?? '0-0';
                $this->reportReaderRecovery();
                $this->read();
                $this->readiness?->resolve(null);
                $this->readiness = null;
            }, $this->retryRead(...));

            return;
        }

        $this->readDeadline = $this->loop->addTimer($this->blockMs / 1000 + 5.0, function (TimerInterface $timer): void {
            if ($this->readDeadline === $timer) {
                $this->readDeadline = null;
                $this->disconnect($this->reader);
            }
        });

        $this->reader->xread('COUNT', static::READ_COUNT, 'BLOCK', $this->blockMs, 'STREAMS', $this->streamKey, $this->cursor)->then(function (?array $streams): void {
            $this->cancelReadDeadline();
            $entries = $streams[0][1] ?? [];

            if (count($entries) < min($this->maxLength, static::READ_COUNT)) {
                $this->deliver($entries);

                return;
            }

            $this->hasGap(EventId::tryParse($this->cursor), $this->reader, allowMissing: false)->then(function (bool $gap) use ($entries): void {
                if ($gap && ! $this->readerGapReported) {
                    $this->notifyGapListeners();
                }

                $this->deliver($entries);
            }, $this->retryRead(...));
        }, $this->retryRead(...));
    }

    /**
     * Deliver a verified batch in stream order and resume reading.
     *
     * @param  list<array{string, list<string>}>  $entries
     */
    protected function deliver(array $entries): void
    {
        if ($this->closed) {
            return;
        }

        $this->reportReaderRecovery();

        try {
            foreach ($entries as $entry) {
                $this->cursor = $entry[0];
                $event = $this->decode($entry);

                if ($event === null) {
                    continue;
                }

                foreach ($this->listeners as $listener) {
                    try {
                        $listener($event);
                    } catch (Throwable $error) {
                        ErrorReporter::report($error);
                    }

                    if ($this->closed) {
                        return;
                    }
                }
            }
        } finally {
            $this->read();
        }
    }

    /**
     * Cancel the deadline after a blocking read finishes.
     */
    protected function cancelReadDeadline(): void
    {
        if ($this->readDeadline !== null) {
            $this->loop->cancelTimer($this->readDeadline);
            $this->readDeadline = null;
        }
    }

    /**
     * Report the first successful response after a reader failure.
     */
    protected function reportReaderRecovery(): void
    {
        if ($this->readerDisconnected) {
            $this->readerDisconnected = false;
            $this->readerGapReported = false;
            error_log('Murmur Redis reader recovered.');
        }
    }

    /**
     * Retry the read after one second, preserving the current cursor.
     */
    protected function retryRead(): void
    {
        $this->cancelReadDeadline();

        if ($this->closed) {
            return;
        }

        if (! $this->readerDisconnected) {
            $this->readerDisconnected = true;
            error_log($this->cursor === null ? 'Murmur is waiting for Redis.' : 'Murmur Redis reader lost its connection.');
        }

        $this->retry = $this->loop->addTimer(1.0, function (TimerInterface $timer): void {
            if ($this->retry !== $timer) {
                return;
            }

            $this->retry = null;

            if ($this->cursor !== null) {
                $this->hasGap(EventId::tryParse($this->cursor), $this->reader)->then(function (bool $gap): void {
                    if ($gap && ! $this->readerGapReported) {
                        $this->readerGapReported = true;
                        $this->notifyGapListeners();
                    }

                    $this->read();
                }, $this->retryRead(...));

                return;
            }

            $this->read();
        });
    }

    /**
     * Notify subscribers before the reader resumes delivering events.
     */
    protected function notifyGapListeners(): void
    {
        foreach ($this->gapListeners as $listener) {
            try {
                $listener();
            } catch (Throwable $error) {
                ErrorReporter::report($error);
            }

            if ($this->closed) {
                return;
            }
        }
    }

    /**
     * Decode a Redis stream entry, returning null when it is malformed.
     *
     * @param  array{string, list<string>}  $entry
     */
    protected function decode(array $entry): ?Event
    {
        [$id, $fields] = $entry;
        $id = EventId::tryParse($id);
        $values = [];

        for ($offset = 0; $offset < count($fields); $offset += 2) {
            $values[$fields[$offset]] = $fields[$offset + 1];
        }

        if ($id === null || ! isset($values['topic'], $values['name'], $values['data']) || strlen($values['data']) > $this->maxPayloadBytes || ! Event::isPublishableName($values['name'])) {
            return null;
        }

        try {
            $data = json_decode($values['data'], flags: JSON_THROW_ON_ERROR);
            json_encode(['topic' => $values['topic'], 'data' => $data], JSON_THROW_ON_ERROR);

            return new Event($values['topic'], $values['name'], $data, $id, $values['except'] ?? null);
        } catch (InvalidArgumentException|JsonException) {
            return null;
        }
    }
}
