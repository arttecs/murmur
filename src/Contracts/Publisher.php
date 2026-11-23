<?php

namespace Arttecs\Murmur\Contracts;

use Arttecs\Murmur\Exceptions\PublishFailed;
use Arttecs\Murmur\Server\Event;

/**
 * Publish events to the Murmur server.
 */
interface Publisher
{
    /**
     * Publish the given event.
     *
     * @throws PublishFailed
     */
    public function publish(Event $event): void;
}
