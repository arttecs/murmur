<?php

namespace Arttecs\Murmur\Server\Log;

use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\Server\Concerns\ReportsErrors;
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
use RuntimeException;
use Throwable;

use function React\Promise\reject;
use function React\Promise\resolve;

/**
 * Replay and subscribe to events stored in a Redis stream.
 */
class RedisEventLog implements EventLog
{
    use ReportsErrors;

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
     * Indicates whether the log has been closed.
     */
    protected bool $closed = false;

    /**
     * Create an event log with independent Redis connections for reading and replay.
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        string $redisUrl,
        protected string $streamKey,
        int $maxLength = 10000,
        ?LoopInterface $loop = null,
        protected int $maxPayloadBytes = 65536,
    ) {
        if ($maxLength < 1) {
            throw new InvalidArgumentException('The event log capacity must be at least 1.');
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
     * @return PromiseInterface<Replay>
     */
    public function since(EventId $after, array $topics, int $limit): PromiseInterface
    {
        return $this->replay->xrevrange($this->streamKey, '+', '-', 'COUNT', 1)->then(function (array $newest) use ($after, $topics, $limit): PromiseInterface {
            $tail = isset($newest[0]) ? EventId::tryParse($newest[0][0]) : null;

            return $this->readReplayPage((string) $after, $topics, $limit)->then(function (?array $events) use ($after, $tail): PromiseInterface|Replay {
                if ($events === null) {
                    return new Replay(true, [], $tail);
                }

                return $this->hasGap($after)->then(fn (bool $gap): Replay => new Replay($gap, $events, $tail));
            });
        });
    }

    /**
     * Read at most one page of Redis entries at a time and keep only matching events.
     *
     * @param  list<string>  $topics
     * @param  list<Event>  $events
     * @return PromiseInterface<list<Event>|null>
     */
    protected function readReplayPage(string $cursor, array $topics, int $limit, array $events = []): PromiseInterface
    {
        return $this->replay->xrange($this->streamKey, '('.$cursor, '+', 'COUNT', 500)->then(function (array $entries) use ($topics, $limit, $events): PromiseInterface|array|null {
            foreach ($entries as $entry) {
                $event = $this->decode($entry);

                if ($event !== null && in_array($event->topic, $topics, true)) {
                    if (count($events) === $limit) {
                        return null;
                    }

                    $events[] = $event;
                }
            }

            return count($entries) === 500
                ? $this->readReplayPage($entries[499][0], $topics, $limit, $events)
                : $events;
        });
    }

    /**
     * Check whether Redis removed entries before the requested cursor.
     *
     * @return PromiseInterface<bool>
     */
    protected function hasGap(EventId $after): PromiseInterface
    {
        return $this->replay->xinfo('STREAM', $this->streamKey)->then(function (array $fields) use ($after): bool {
            $info = array_column(array_chunk($fields, 2), 1, 0);
            $firstEntry = $info['first-entry'];

            if ($firstEntry === null) {
                return false;
            }

            $first = EventId::tryParse($firstEntry[0]);

            return $first === null || ($info['entries-added'] > $info['length'] && $after->compare($first) < 0);
        }, function (Throwable $error): bool {
            if ($error instanceof ErrorReply && $error->getMessage() === 'ERR no such key') {
                return false;
            }

            throw $error;
        });
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
     * Stop reading and close both Redis connections.
     */
    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;
        $this->listeners = [];

        if ($this->retry !== null) {
            $this->loop->cancelTimer($this->retry);
            $this->retry = null;
        }

        $this->reader->close();
        $this->replay->close();
        $this->readiness?->reject(new RuntimeException('The Redis event log closed before it was ready.'));
        $this->readiness = null;
    }

    /**
     * Resolve the starting cursor or read new stream entries from the current cursor.
     */
    protected function read(): void
    {
        if ($this->closed) {
            return;
        }

        if ($this->cursor === null) {
            $this->reader->xrevrange($this->streamKey, '+', '-', 'COUNT', 1)->then(function (array $entries): void {
                $this->cursor = $entries[0][0] ?? '0-0';
                $this->read();
                $this->readiness?->resolve(null);
                $this->readiness = null;
            }, $this->retryRead(...));

            return;
        }

        $this->reader->xread('BLOCK', 5000, 'STREAMS', $this->streamKey, $this->cursor)->then(function (?array $streams): void {
            try {
                foreach ($streams[0][1] ?? [] as $entry) {
                    $this->cursor = $entry[0];
                    $event = $this->decode($entry);

                    if ($event === null) {
                        continue;
                    }

                    foreach ($this->listeners as $listener) {
                        try {
                            $listener($event);
                        } catch (Throwable $error) {
                            $this->reportError($error);
                        }

                        if ($this->closed) {
                            return;
                        }
                    }
                }
            } finally {
                $this->read();
            }
        }, $this->retryRead(...));
    }

    /**
     * Retry the read after one second, preserving the current cursor.
     */
    protected function retryRead(): void
    {
        if ($this->closed) {
            return;
        }

        $this->retry = $this->loop->addTimer(1.0, function (): void {
            $this->retry = null;
            $this->read();
        });
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

        if ($id === null || ! isset($values['topic'], $values['name'], $values['data']) || strlen($values['data']) > $this->maxPayloadBytes) {
            return null;
        }

        try {
            return new Event($values['topic'], $values['name'], json_decode($values['data'], flags: JSON_THROW_ON_ERROR), $id);
        } catch (InvalidArgumentException|JsonException) {
            return null;
        }
    }
}
