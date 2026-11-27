<?php

namespace Arttecs\Murmur\Tests\Fixtures;

use Illuminate\Broadcasting\InteractsWithSockets;
use Illuminate\Broadcasting\PrivateChannel;
use Illuminate\Contracts\Broadcasting\ShouldBroadcastNow;

/**
 * Broadcast a shipment update to the order's private channel.
 */
class OrderShipped implements ShouldBroadcastNow
{
    use InteractsWithSockets;

    /**
     * Create an order shipment event.
     */
    public function __construct(public int $orderId, ?string $socket = null)
    {
        $this->socket = $socket;
    }

    /**
     * Get the private order channel.
     */
    public function broadcastOn(): PrivateChannel
    {
        return new PrivateChannel('orders.'.$this->orderId);
    }
}
