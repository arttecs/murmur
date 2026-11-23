<?php

namespace Arttecs\Murmur\Server\Http;

use Arttecs\Murmur\Server\Connection;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Sse\Frame;
use React\Stream\WritableStreamInterface;

/**
 * Hold frames from the hub until replay has caught up.
 */
class HeldConnection extends Connection
{
    /**
     * The live events and heartbeat frames held until replay finishes, or null once delivery is live.
     *
     * @var list<Event|string>|null
     */
    protected ?array $held = [];

    /**
     * The number of bytes queued while replay is pending.
     */
    protected int $heldBytes = 0;

    /**
     * Create a held connection starting at the client's last received event ID.
     */
    public function __construct(WritableStreamInterface $stream, int $maxBufferBytes = 1048576, protected ?EventId $lastWrittenId = null)
    {
        parent::__construct($stream, $maxBufferBytes);
    }

    /**
     * Hold incoming hub frames during replay and send them directly afterward.
     */
    public function send(string $frame): void
    {
        if ($this->held === null) {
            parent::send($frame);

            return;
        }

        $this->hold($frame, strlen($frame));
    }

    /**
     * Hold an unseen live event during replay or write it directly afterward.
     *
     * @throws \JsonException
     */
    public function sendEvent(Event $event): void
    {
        if ($this->held === null) {
            $this->writeEvent($event);

            return;
        }

        if (! $this->hasWritten($event)) {
            $this->hold($event, strlen(Frame::event($event)));
        }
    }

    /**
     * Write an initial protocol frame ahead of replay and the held live events.
     */
    public function sendImmediately(string $frame): void
    {
        parent::send($frame);
    }

    /**
     * Write replayed events, drain held events and frames, and switch to live delivery.
     *
     * @param  list<Event>  $replay
     *
     * @throws \JsonException
     */
    public function release(array $replay = []): void
    {
        if ($this->held === null) {
            return;
        }

        foreach ($replay as $event) {
            if ($this->closed) {
                return;
            }

            $this->writeEvent($event);
        }

        for ($index = 0; isset($this->held[$index]); $index++) {
            $item = $this->held[$index];

            if ($item instanceof Event) {
                $this->writeEvent($item);
            } else {
                $this->sendImmediately($item);
            }
        }

        $this->held = null;
        $this->heldBytes = 0;
    }

    /**
     * Determine if the event is at or before the last written ID.
     */
    protected function hasWritten(Event $event): bool
    {
        return $event->id !== null && $this->lastWrittenId !== null && $event->id->compare($this->lastWrittenId) <= 0;
    }

    /**
     * Write an unseen event and advance the cursor for the rest of the connection.
     *
     * @throws \JsonException
     */
    protected function writeEvent(Event $event): void
    {
        if ($this->closed || $this->hasWritten($event)) {
            return;
        }

        $frame = Frame::event($event);
        $this->lastWrittenId = $event->id ?? $this->lastWrittenId;
        parent::send($frame);
    }

    /**
     * Queue an event or heartbeat, closing the connection if the queue exceeds its byte limit.
     */
    protected function hold(Event|string $item, int $bytes): void
    {
        $this->heldBytes += $bytes;

        if ($this->heldBytes > $this->maxBufferBytes) {
            $this->close();

            return;
        }

        $this->held[] = $item;
    }

    /**
     * Discard the held events and frames and close the stream.
     */
    public function close(): void
    {
        $this->held = null;
        $this->heldBytes = 0;

        parent::close();
    }
}
