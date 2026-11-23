<?php

namespace Arttecs\Murmur\Server\Log;

use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\Server\ErrorReporter;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\EventIdGenerator;
use Arttecs\Murmur\Server\Replay;
use InvalidArgumentException;
use React\Promise\PromiseInterface;
use Throwable;

use function React\Promise\resolve;

/**
 * Store a bounded history of events in memory.
 */
class MemoryEventLog implements EventLog
{
    /**
     * The generator used to assign event identifiers.
     */
    protected EventIdGenerator $ids;

    /**
     * The identifier marking the lower boundary of replayable history.
     */
    protected EventId $floor;

    /**
     * The events held in the circular buffer.
     *
     * @var list<Event>
     */
    protected array $events = [];

    /**
     * The buffer position where the next event will be stored.
     */
    protected int $next = 0;

    /**
     * The listeners notified when an event is appended.
     *
     * @var list<callable(Event): void>
     */
    protected array $listeners = [];

    /**
     * Create a new in-memory event log.
     *
     * @throws InvalidArgumentException
     */
    public function __construct(protected int $capacity = 10000, ?EventIdGenerator $ids = null)
    {
        if ($capacity < 1) {
            throw new InvalidArgumentException('The event log capacity must be at least 1.');
        }

        $this->ids = $ids ?? new EventIdGenerator;
        $this->floor = $this->ids->next();
    }

    /**
     * Assign an identifier, store the event, and notify listeners.
     */
    public function append(Event $event): Event
    {
        $event = $event->withId($this->ids->next());

        if (isset($this->events[$this->next])) {
            $this->floor = $this->events[$this->next]->id;
        }

        $this->events[$this->next] = $event;
        $this->next = ($this->next + 1) % $this->capacity;

        foreach ($this->listeners as $listener) {
            try {
                $listener($event);
            } catch (Throwable $error) {
                ErrorReporter::report($error);
            }
        }

        return $event;
    }

    /**
     * Replay retained events after the given identifier for the requested topics.
     *
     * @param  list<string>  $topics
     * @return PromiseInterface<Replay>
     */
    public function since(EventId $after, array $topics, int $limit): PromiseInterface
    {
        $events = [];
        $count = count($this->events);
        $oldest = $count === $this->capacity ? $this->next : 0;
        $tail = $count === 0 ? null : $this->events[($this->next + $this->capacity - 1) % $this->capacity]->id;

        for ($offset = 0; $offset < $count; $offset++) {
            $event = $this->events[($oldest + $offset) % $this->capacity];

            if ($event->id->compare($after) > 0 && in_array($event->topic, $topics, true)) {
                if (count($events) === $limit) {
                    return resolve(new Replay(true, [], $tail));
                }

                $events[] = $event;
            }
        }

        return resolve(new Replay($after->compare($this->floor) < 0, $events, $tail));
    }

    /**
     * Register a listener that receives events with their assigned identifiers.
     *
     * @param  callable(Event): void  $listener
     */
    public function subscribe(callable $listener): void
    {
        $this->listeners[] = $listener;
    }

    /**
     * Indicate that the in-memory log can deliver live events immediately.
     *
     * @return PromiseInterface<null>
     */
    public function ready(): PromiseInterface
    {
        return resolve(null);
    }

    /**
     * Drop all registered listeners.
     */
    public function close(): void
    {
        $this->listeners = [];
    }
}
