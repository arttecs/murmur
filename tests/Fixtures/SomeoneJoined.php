<?php

namespace Arttecs\Murmur\Tests\Fixtures;

use Illuminate\Broadcasting\PresenceChannel;
use Illuminate\Contracts\Broadcasting\ShouldBroadcastNow;

/**
 * Broadcast a join event to a room's presence channel.
 */
class SomeoneJoined implements ShouldBroadcastNow
{
    /**
     * Get the room's presence channel.
     */
    public function broadcastOn(): PresenceChannel
    {
        return new PresenceChannel('room.1');
    }
}
