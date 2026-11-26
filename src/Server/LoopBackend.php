<?php

namespace Arttecs\Murmur\Server;

use InvalidArgumentException;
use React\EventLoop\LoopInterface;
use React\EventLoop\StreamSelectLoop;

/**
 * Check the socket limit imposed by the event loop backend.
 *
 * @internal
 */
class LoopBackend
{
    /**
     * Use the backend selected for this server.
     *
     * @param  class-string<LoopInterface>  $loop
     */
    public function __construct(protected string $loop) {}

    /**
     * Describe the limit of the stream_select backend.
     */
    public function notice(): ?string
    {
        return is_a($this->loop, StreamSelectLoop::class, true)
            ? 'The stream_select event loop limits connections to about 1000 sockets. Install ext-uv, ext-ev or ext-event to lift this limit.'
            : null;
    }

    /**
     * Reject socket limits the stream_select backend cannot support.
     *
     * @throws InvalidArgumentException
     */
    public function validate(int $maxSockets): void
    {
        if (is_a($this->loop, StreamSelectLoop::class, true) && $maxSockets > 1000) {
            throw new InvalidArgumentException('server.max_sockets cannot exceed 1000 with stream_select. Install ext-uv, ext-ev or ext-event to lift this limit.');
        }
    }
}
