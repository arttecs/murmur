<?php

namespace Arttecs\Murmur\Server;

use Arttecs\Murmur\Server\Sse\Frame;
use React\Stream\WritableStreamInterface;

/**
 * Manage an open server-sent event stream.
 */
class Connection
{
    /**
     * The number of bytes written under backpressure since the last drain.
     */
    protected int $bufferedBytes = 0;

    /**
     * Indicates whether the connection has closed.
     */
    protected bool $closed = false;

    /**
     * The listeners to notify when the connection closes.
     *
     * @var list<callable(): void>
     */
    protected array $closeListeners = [];

    /**
     * Create a connection and listen for stream events.
     */
    public function __construct(protected WritableStreamInterface $stream, protected int $maxBufferBytes = 1048576)
    {
        $this->stream->on('drain', function (): void {
            $this->bufferedBytes = 0;
        });

        $this->stream->on('close', $this->close(...));
    }

    /**
     * Write a frame and close the connection if it exceeds the backpressure limit.
     */
    public function send(string $frame): void
    {
        if ($this->closed || $this->stream->write($frame)) {
            return;
        }

        $this->bufferedBytes += strlen($frame);

        if ($this->bufferedBytes > $this->maxBufferBytes) {
            $this->close();
        }
    }

    /**
     * Encode and write an event to the stream.
     *
     * @throws \JsonException
     */
    public function sendEvent(Event $event): void
    {
        $this->send(Frame::event($event));
    }

    /**
     * Close the stream and notify the connection's listeners.
     */
    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;
        $this->stream->close();

        $listeners = $this->closeListeners;
        $this->closeListeners = [];

        foreach ($listeners as $listener) {
            $listener();
        }
    }

    /**
     * Register a listener for the connection's close event, or call it now if already closed.
     *
     * @param  callable(): void  $listener
     */
    public function onClose(callable $listener): void
    {
        if ($this->closed) {
            $listener();

            return;
        }

        $this->closeListeners[] = $listener;
    }

    /**
     * Determine if the connection is closed.
     */
    public function isClosed(): bool
    {
        return $this->closed;
    }
}
