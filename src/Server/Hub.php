<?php

namespace Arttecs\Murmur\Server;

use Arttecs\Murmur\Server\Sse\Frame;
use InvalidArgumentException;
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
     * @var SplObjectStorage<Connection, array{topics: list<string>, startedAt: int|float|null, slice: int}>
     */
    protected SplObjectStorage $connections;

    /**
     * The connections grouped by heartbeat slice.
     *
     * @var list<SplObjectStorage<Connection, null>>
     */
    protected array $heartbeatBuckets = [];

    /**
     * The index of the next heartbeat bucket to send.
     */
    protected int $nextHeartbeatSlice = 0;

    /**
     * The newest event identifier dispatched across all topics.
     */
    protected ?EventId $lastDispatchedId = null;

    /**
     * Create a hub with no connections.
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        protected ?Metrics $metrics = null,
        protected ?LifecycleRecorder $lifecycle = null,
        int $heartbeatSlices = 1,
    ) {
        if ($heartbeatSlices < 1) {
            throw new InvalidArgumentException('The hub needs at least one heartbeat slice.');
        }

        $this->connections = new SplObjectStorage;

        for ($slice = 0; $slice < $heartbeatSlices; $slice++) {
            $this->heartbeatBuckets[] = new SplObjectStorage;
        }
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
            ['startedAt' => $startedAt, 'slice' => $slice] = $this->connections[$connection];
            $this->removeSubscriptions($connection);
            unset($this->connections[$connection]);
        } else {
            $connection->onClose(function (string $reason) use ($connection): void {
                $this->recordClose($connection, $reason);
                $this->detach($connection);
            });

            if ($this->lifecycle !== null) {
                $startedAt = hrtime(true);
                $this->recordLifecycle($connection, $topics);
            }

            $slice = $this->heartbeatSliceFor();
            $this->heartbeatBuckets[$slice][$connection] = null;
        }

        $this->connections[$connection] = compact('topics', 'startedAt', 'slice');

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

        $slice = $this->connections[$connection]['slice'];
        $this->removeSubscriptions($connection);
        unset($this->connections[$connection], $this->heartbeatBuckets[$slice][$connection]);
    }

    /**
     * Choose the heartbeat slice for a new connection.
     *
     * Only the half of the rotation that fired most recently is eligible, so the
     * first heartbeat arrives between half an interval and a full interval after
     * attachment. The least loaded eligible slice wins, which spreads a burst of
     * connections, and a connection keeps its slice for life so its cadence never shifts.
     */
    protected function heartbeatSliceFor(): int
    {
        $count = count($this->heartbeatBuckets);
        $chosen = null;

        for ($age = 0; $age < max(1, intdiv($count, 2)); $age++) {
            $slice = (($this->nextHeartbeatSlice - 1 - $age) % $count + $count) % $count;

            if ($chosen === null || count($this->heartbeatBuckets[$slice]) < count($this->heartbeatBuckets[$chosen])) {
                $chosen = $slice;
            }
        }

        return $chosen;
    }

    /**
     * Remove a connection from each subscribed topic.
     */
    protected function removeSubscriptions(Connection $connection): void
    {
        foreach ($this->connections[$connection]['topics'] as $topic) {
            unset($this->topics[$topic][$connection]);

            if (count($this->topics[$topic]) === 0) {
                unset($this->topics[$topic]);
            }
        }
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
     * Send a heartbeat to every attached connection.
     */
    public function heartbeat(): void
    {
        $this->heartbeatConnections($this->connections);
    }

    /**
     * Get the number of slices in a heartbeat rotation.
     */
    public function heartbeatSliceCount(): int
    {
        return count($this->heartbeatBuckets);
    }

    /**
     * Send a heartbeat to the next slice of attached connections.
     */
    public function heartbeatSlice(): void
    {
        $slice = $this->nextHeartbeatSlice;
        $this->nextHeartbeatSlice = ($slice + 1) % $this->heartbeatSliceCount();
        $this->heartbeatConnections($this->heartbeatBuckets[$slice]);
    }

    /**
     * Share one numbered heartbeat and one fallback comment across the given connections.
     *
     * @param  SplObjectStorage<Connection, mixed>  $connections
     */
    protected function heartbeatConnections(SplObjectStorage $connections): void
    {
        $position = $this->lastDispatchedId;
        $comment = Frame::comment();

        if ($position === null) {
            $this->sendTo($connections, fn (Connection $connection) => $connection->send($comment));

            return;
        }

        $frame = Frame::heartbeat($position);
        $this->sendTo($connections, fn (Connection $connection) => $connection->sendHeartbeat($position, $frame, $comment));
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
