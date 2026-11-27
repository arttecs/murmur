<?php

namespace Arttecs\Murmur\Tests\Fixtures;

use Illuminate\Broadcasting\Channel;
use Illuminate\Contracts\Broadcasting\ShouldBroadcastNow;

/**
 * Broadcast a named event to the public news channel.
 */
class NewsUpdated implements ShouldBroadcastNow
{
    /**
     * Get the public news channel.
     */
    public function broadcastOn(): Channel
    {
        return new Channel('news');
    }

    /**
     * Get the name used for the broadcast.
     */
    public function broadcastAs(): string
    {
        return 'updated';
    }
}
