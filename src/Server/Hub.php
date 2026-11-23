<?php

namespace Arttecs\Murmur\Server;

use Arttecs\Murmur\Server\Sse\Frame;
use SplObjectStorage;

/**
 * Distribute server-sent events to subscribed connections.
 */
class Hub
{
    /**
     * The connections subscribed to each topic.
     *
     * @var array<string, SplObjectStorage<Connection, null>>
     */
    private array $topics = [];

    /**
     * The topics subscribed to by each connection.
     *
     * @var SplObjectStorage<Connection, array<array-key, string>>
     */
    private SplObjectStorage $connections;

    /**
     * Create a hub with no connections.
     */
    public function __construct()
    {
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

        if (isset($this->connections[$connection])) {
            $this->detach($connection);
        } else {
            $connection->onClose(function () use ($connection): void {
                $this->detach($connection);
            });
        }

        $topics = array_unique($topics);
        $this->connections[$connection] = $topics;

        foreach ($topics as $topic) {
            $this->topics[$topic] ??= new SplObjectStorage;
            $this->topics[$topic][$connection] = null;
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

        foreach ($this->connections[$connection] as $topic) {
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
        $connections = $this->topics[$event->topic] ?? null;

        if ($connections === null) {
            return;
        }

        $frame = Frame::event($event);

        foreach (iterator_to_array($connections, false) as $connection) {
            $connection->send($frame);
        }
    }

    /**
     * Send a heartbeat frame to all connections.
     */
    public function heartbeat(): void
    {
        $frame = Frame::comment();

        foreach (iterator_to_array($this->connections, false) as $connection) {
            $connection->send($frame);
        }
    }

    /**
     * Get the number of attached connections.
     */
    public function connectionCount(): int
    {
        return count($this->connections);
    }

    /**
     * Close every attached connection.
     */
    public function closeAll(): void
    {
        foreach (iterator_to_array($this->connections, false) as $connection) {
            $connection->close();
        }
    }
}
