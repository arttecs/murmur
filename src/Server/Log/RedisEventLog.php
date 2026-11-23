<?php

namespace Arttecs\Murmur\Server\Log;

use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Replay;
use Clue\React\Redis\Client;
use Clue\React\Redis\Factory;
use InvalidArgumentException;
use JsonException;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\EventLoop\TimerInterface;
use React\Promise\PromiseInterface;

use function React\Promise\all;

/**
 * Replay and subscribe to events stored in a Redis stream.
 */
class RedisEventLog implements EventLog
{
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
     */
    public function __construct(string $redisUrl, protected string $streamKey, protected int $maxLength = 10000, ?LoopInterface $loop = null)
    {
        $this->loop = $loop ?? Loop::get();
        $factory = new Factory($this->loop);
        $this->reader = $factory->createLazyClient($redisUrl);
        $this->replay = $factory->createLazyClient($redisUrl);
    }

    /**
     * Replay retained events after the given identifier for the requested topics.
     *
     * @param  list<string>  $topics
     * @return PromiseInterface<Replay>
     */
    public function since(EventId $after, array $topics): PromiseInterface
    {
        return all([
            $this->replay->xrange($this->streamKey, '('.$after, '+'),
            $this->replay->xrange($this->streamKey, '-', '+', 'COUNT', 1),
            $this->replay->xlen($this->streamKey),
        ])->then(function (array $results) use ($after, $topics): Replay {
            [$entries, $oldest, $length] = $results;
            $first = isset($oldest[0]) ? EventId::tryParse($oldest[0][0]) : null;
            $gap = $length >= $this->maxLength && $first !== null && $after->compare($first) < 0;
            $events = [];

            foreach ($entries as $entry) {
                $event = $this->decode($entry);

                if ($event !== null && in_array($event->topic, $topics, true)) {
                    $events[] = $event;
                }
            }

            return new Replay($gap, $events);
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
            }, $this->retryRead(...));

            return;
        }

        $this->reader->xread('BLOCK', 5000, 'STREAMS', $this->streamKey, $this->cursor)->then(function (?array $streams): void {
            foreach ($streams[0][1] ?? [] as $entry) {
                $this->cursor = $entry[0];
                $event = $this->decode($entry);

                if ($event === null) {
                    continue;
                }

                foreach ($this->listeners as $listener) {
                    $listener($event);

                    if ($this->closed) {
                        return;
                    }
                }
            }

            $this->read();
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

        if ($id === null || ! isset($values['topic'], $values['name'], $values['data'])) {
            return null;
        }

        try {
            return new Event($values['topic'], $values['name'], json_decode($values['data'], true, flags: JSON_THROW_ON_ERROR), $id);
        } catch (InvalidArgumentException|JsonException) {
            return null;
        }
    }
}
