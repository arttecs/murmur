<?php

namespace Arttecs\Murmur\Server\Http;

use Arttecs\Murmur\Server\Connection;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Metrics;
use Arttecs\Murmur\Server\Replay;
use Arttecs\Murmur\Server\Sse\Frame;
use React\Stream\WritableStreamInterface;

/**
 * Hold frames from the hub until replay has caught up.
 *
 * @internal
 */
class HeldConnection extends Connection
{
    /**
     * The live events and heartbeat frames held until replay finishes, or null once delivery is live.
     *
     * @var list<array{Event, string}|string>|null
     */
    protected ?array $held = [];

    /**
     * The number of bytes queued while replay is pending.
     */
    protected int $heldBytes = 0;

    /**
     * The last event ID written to the subscriber.
     */
    protected ?EventId $lastWrittenId = null;

    /**
     * Create a held connection with the subscriber's requested replay cursor.
     */
    public function __construct(
        WritableStreamInterface $stream,
        int $maxBufferBytes = 1048576,
        protected ?EventId $cursor = null,
        ?Metrics $metrics = null,
        ?string $id = null,
    ) {
        parent::__construct($stream, $maxBufferBytes, $metrics, $id);
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
    public function sendEvent(Event $event, ?string $frame = null): void
    {
        if ($this->excludes($event)) {
            return;
        }

        if ($this->held === null) {
            $this->writeEvent($event, $frame);

            return;
        }

        $frame ??= Frame::event($event);
        $this->hold([$event, $frame], strlen($frame));
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
     * @throws \JsonException
     */
    public function release(?Replay $replay = null): void
    {
        if ($this->held === null) {
            return;
        }

        $tail = $replay?->tail;

        if ($this->cursor !== null && $tail !== null) {
            $this->lastWrittenId = $this->cursor->compare($tail) <= 0 ? $this->cursor : $tail;
        }

        $frames = [];
        $bytes = 0;
        $gap = $replay?->gap ?? false;

        foreach ($replay?->events ?? [] as $event) {
            if ($this->excludes($event)) {
                continue;
            }

            $frame = Frame::event($event);
            $bytes += strlen($frame);

            if ($bytes > $this->maxBufferBytes / 2) {
                $gap = true;
                $frames = [];

                break;
            }

            $frames[] = [$event, $frame];
        }

        if ($gap) {
            $this->sendImmediately(Frame::gap());
        }

        foreach ($frames as [$event, $frame]) {
            $this->writeEvent($event, $frame, replayed: true);
        }

        for ($index = 0; isset($this->held[$index]); $index++) {
            $item = $this->held[$index];

            if (is_array($item)) {
                [$event, $frame] = $item;
                $this->writeEvent($event, $frame);
            } elseif (! $gap || $item !== Frame::gap()) {
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
    protected function writeEvent(Event $event, ?string $frame = null, bool $replayed = false): void
    {
        if ($this->closed || $this->hasWritten($event)) {
            return;
        }

        $frame ??= Frame::event($event);
        $this->lastWrittenId = $event->id ?? $this->lastWrittenId;
        $this->writeEventFrame($frame, $replayed);
    }

    /**
     * Queue an event or heartbeat, closing the connection if the queue exceeds its byte limit.
     *
     * @param  array{Event, string}|string  $item
     */
    protected function hold(array|string $item, int $bytes): void
    {
        $this->heldBytes += $bytes;

        if ($this->heldBytes > $this->maxBufferBytes) {
            $this->metrics?->increment('murmur_slow_consumers_total');
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
