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
    public function __construct(
        protected WritableStreamInterface $stream,
        protected int $maxBufferBytes = 1048576,
        protected ?Metrics $metrics = null,
    ) {
        $this->metrics?->increment('murmur_connections_opened_total');

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
        if ($this->writeFrame($frame) && $frame === Frame::gap()) {
            $this->metrics?->increment('murmur_gaps_total');
        }
    }

    /**
     * Hand a frame to a writable stream and track bytes and backpressure.
     */
    protected function writeFrame(string $frame): bool
    {
        if ($this->closed || ! $this->stream->isWritable()) {
            return false;
        }

        $writable = $this->stream->write($frame);
        $bytes = strlen($frame);
        $this->metrics?->increment('murmur_bytes_sent_total', $bytes);

        if (! $writable) {
            $this->bufferedBytes += $bytes;

            if ($this->bufferedBytes > $this->maxBufferBytes) {
                $this->metrics?->increment('murmur_slow_consumers_total');
                $this->close();
            }
        }

        return true;
    }

    /**
     * Encode and write an event to the stream.
     *
     * @throws \JsonException
     */
    public function sendEvent(Event $event, ?string $frame = null): void
    {
        $this->writeEventFrame($frame ?? Frame::event($event));
    }

    /**
     * Write an event frame and record whether it came from replay.
     */
    protected function writeEventFrame(string $frame, bool $replayed = false): void
    {
        if ($this->writeFrame($frame)) {
            $this->metrics?->increment('murmur_events_sent_total');

            if ($replayed) {
                $this->metrics?->increment('murmur_replayed_events_total');
            }
        }
    }

    /**
     * End the stream and let buffered output flush.
     */
    public function end(): void
    {
        $this->stream->end();
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
        $this->metrics?->increment('murmur_connections_closed_total');
        try {
            $this->stream->close();
        } finally {
            $listeners = $this->closeListeners;
            $this->closeListeners = [];

            foreach ($listeners as $listener) {
                $listener();
            }
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
