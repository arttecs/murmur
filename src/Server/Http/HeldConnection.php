<?php

namespace Arttecs\Murmur\Server\Http;

use Arttecs\Murmur\Server\Connection;
use Arttecs\Murmur\Server\EventId;

/**
 * Hold frames from the hub until replay has caught up.
 */
class HeldConnection extends Connection
{
    /**
     * The live frames held until replay finishes, or null once delivery is live.
     *
     * @var list<string>|null
     */
    protected ?array $heldFrames = [];

    /**
     * The number of bytes queued while replay is pending.
     */
    protected int $heldBytes = 0;

    /**
     * Hold incoming hub frames during replay and send them directly afterward.
     */
    public function send(string $frame): void
    {
        if ($this->heldFrames === null) {
            parent::send($frame);

            return;
        }

        $this->heldBytes += strlen($frame);

        if ($this->heldBytes > $this->maxBufferBytes) {
            $this->close();

            return;
        }

        $this->heldFrames[] = $frame;
    }

    /**
     * Write a protocol or replay frame ahead of the held live frames.
     */
    public function sendImmediately(string $frame): void
    {
        parent::send($frame);
    }

    /**
     * Drain held frames, discarding event IDs already covered by replay.
     */
    public function release(?EventId $last): void
    {
        for ($index = 0; isset($this->heldFrames[$index]); $index++) {
            $frame = $this->heldFrames[$index];

            if ($last !== null && str_starts_with($frame, 'id: ')) {
                $line = strstr($frame, "\n", true);
                $id = EventId::tryParse(substr($line, 4));

                if ($id !== null && $id->compare($last) <= 0) {
                    continue;
                }
            }

            $this->sendImmediately($frame);
        }

        $this->heldFrames = null;
        $this->heldBytes = 0;
    }

    /**
     * Discard the held frames and close the stream.
     */
    public function close(): void
    {
        $this->heldFrames = null;
        $this->heldBytes = 0;

        parent::close();
    }
}
