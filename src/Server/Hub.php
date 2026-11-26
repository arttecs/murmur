<?php

namespace Arttecs\Murmur\Server;

use Arttecs\Murmur\Server\Sse\Frame;
use SplObjectStorage;
use Throwable;

/**
 * Distribute server-sent events to subscribed connections.
 *
 * @internal
 */
class Hub
{
    /**
     * The connections subscribed to each topic.
     *
     * @var array<string, SplObjectStorage<Connection, null>>
     */
    protected array $topics = [];

    /**
     * The topics subscribed to by each connection.
     *
     * @var SplObjectStorage<Connection, array{topics: list<string>, startedAt: int|float|null}>
     */
    protected SplObjectStorage $connections;

    /**
     * The newest event identifier dispatched across all topics.
     */
    protected ?EventId $lastDispatchedId = null;

    /**
     * Create a hub with no connections.
     */
    public function __construct(
        protected ?Metrics $metrics = null,
        protected ?LifecycleRecorder $lifecycle = null,
    ) {
        $this->connections = new SplObjectStorage;
    }

    /**
     * Attach an open connection and replace its topic subscriptions.
     *
     * @param  array<array-key, string>  $topics
     */
    public function attach(Connection $connection, array $topics): void
    {
        if ($connection->isClosed()) {
            return;
        }

        $startedAt = null;
        $topics = array_values(array_unique($topics));

        if (isset($this->connections[$connection])) {
            $startedAt = $this->connections[$connection]['startedAt'];
            $this->detach($connection);
        } else {
            $connection->onClose(function (string $reason) use ($connection): void {
                $this->recordClose($connection, $reason);
                $this->detach($connection);
            });

            if ($this->lifecycle !== null) {
                $startedAt = hrtime(true);
                $this->recordLifecycle($connection, $topics);
            }
        }

        $this->connections[$connection] = compact('topics', 'startedAt');

        foreach ($topics as $topic) {
            $this->topics[$topic] ??= new SplObjectStorage;
            $this->topics[$topic][$connection] = null;
        }
    }

    /**
     * Record the close of an attached connection.
     */
    protected function recordClose(Connection $connection, string $reason): void
    {
        $attachment = $this->connections[$connection] ?? null;
        $startedAt = $attachment['startedAt'] ?? null;

        if ($startedAt !== null) {
            $this->recordLifecycle($connection, $attachment['topics'], [
                'durationSeconds' => (hrtime(true) - $startedAt) / 1_000_000_000.0,
                'reason' => $reason,
            ]);
        }
    }

    /**
     * Enqueue a lifecycle record and count a drop if the queue cannot retain it.
     *
     * @param  list<string>  $topics
     * @param  array{durationSeconds?: float, reason?: string}  $closed
     */
    protected function recordLifecycle(Connection $connection, array $topics, array $closed = []): void
    {
        $dropped = $this->lifecycle->enqueue([
            'connectionId' => $connection->id,
            'topics' => $topics,
            'occurredAt' => time(),
            ...$closed,
        ]);

        if ($dropped > 0) {
            $this->metrics?->increment('murmur_lifecycle_dropped_total', $dropped);
        }
    }

    /**
     * Remove a connection and all its topic subscriptions.
     */
    public function detach(Connection $connection): void
    {
        if (! isset($this->connections[$connection])) {
            return;
        }

        foreach ($this->connections[$connection]['topics'] as $topic) {
            unset($this->topics[$topic][$connection]);

            if (count($this->topics[$topic]) === 0) {
                unset($this->topics[$topic]);
            }
        }

        unset($this->connections[$connection]);
    }

    /**
     * Send an event frame to the connections subscribed to its topic.
     *
     * @throws \JsonException
     */
    public function dispatch(Event $event): void
    {
        $this->metrics?->increment('murmur_events_received_total');
        $connections = $this->topics[$event->topic] ?? null;

        if ($connections !== null) {
            $frame = Frame::event($event);
            $this->sendTo($connections, fn (Connection $connection) => $connection->sendEvent($event, $frame));
        }

        if ($event->id !== null && ($this->lastDispatchedId === null || $event->id->compare($this->lastDispatchedId) > 0)) {
            $this->lastDispatchedId = $event->id;
        }
    }

    /**
     * Signal lost history to every attached connection.
     */
    public function gap(): void
    {
        $frame = Frame::gap();
        $this->sendTo($this->connections, fn (Connection $connection) => $connection->send($frame));
    }

    /**
     * Send a frame to each connection, closing and reporting failed connections.
     *
     * @param  SplObjectStorage<Connection, mixed>  $connections
     * @param  callable(Connection): void  $send
     */
    protected function sendTo(SplObjectStorage $connections, callable $send): void
    {
        foreach (iterator_to_array($connections, false) as $connection) {
            try {
                $send($connection);
            } catch (Throwable $error) {
                ErrorReporter::report($error);

                try {
                    $connection->close('error');
                } catch (Throwable $closeError) {
                    ErrorReporter::report($closeError);
                }
            }
        }
    }

    /**
     * Share one numbered heartbeat and one fallback comment across the connections.
     */
    public function heartbeat(): void
    {
        $position = $this->lastDispatchedId;
        $comment = Frame::comment();

        if ($position === null) {
            $this->sendTo($this->connections, fn (Connection $connection) => $connection->send($comment));

            return;
        }

        $frame = Frame::heartbeat($position);
        $this->sendTo($this->connections, fn (Connection $connection) => $connection->sendHeartbeat($position, $frame, $comment));
    }

    /**
     * Get the number of attached connections.
     */
    public function connectionCount(): int
    {
        return count($this->connections);
    }

    /**
     * Get the number of topics with at least one subscriber.
     */
    public function topicCount(): int
    {
        return count($this->topics);
    }

    /**
     * Snapshot the connections in attachment order.
     *
     * @return list<Connection>
     */
    public function connections(): array
    {
        return iterator_to_array($this->connections, false);
    }

    /**
     * Close every attached connection.
     */
    public function closeAll(): void
    {
        foreach (iterator_to_array($this->connections, false) as $connection) {
            $connection->close('shutdown');
        }
    }
}
