<?php

namespace Arttecs\Murmur\Server\Log;

use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\EventIdGenerator;
use Arttecs\Murmur\Server\Replay;
use React\Promise\PromiseInterface;

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
     */
    public function __construct(protected int $capacity = 10000, ?EventIdGenerator $ids = null)
    {
        $this->ids = $ids ?? new EventIdGenerator;
        $this->floor = $this->ids->next();
    }

    /**
     * Assign an identifier, store the event, and notify listeners.
     */
    public function append(Event $event): Event
    {
        $id = $this->ids->next();
        $event = $event->withId($id);

        if ($this->capacity > 0) {
            if (isset($this->events[$this->next])) {
                $this->floor = $this->events[$this->next]->id;
            }

            $this->events[$this->next] = $event;
            $this->next = ($this->next + 1) % $this->capacity;
        } else {
            $this->floor = $id;
        }

        foreach ($this->listeners as $listener) {
            $listener($event);
        }

        return $event;
    }

    /**
     * Replay retained events after the given identifier for the requested topics.
     *
     * @param  list<string>  $topics
     * @return PromiseInterface<Replay>
     */
    public function since(EventId $after, array $topics): PromiseInterface
    {
        $events = [];
        $count = count($this->events);
        $oldest = $count === $this->capacity ? $this->next : 0;

        for ($offset = 0; $offset < $count; $offset++) {
            $event = $this->events[($oldest + $offset) % $this->capacity];

            if ($event->id->compare($after) > 0 && in_array($event->topic, $topics, true)) {
                $events[] = $event;
            }
        }

        return resolve(new Replay($after->compare($this->floor) < 0, $events));
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
     * Drop all registered listeners.
     */
    public function close(): void
    {
        $this->listeners = [];
    }
}
