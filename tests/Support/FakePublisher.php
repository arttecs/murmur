<?php

namespace Arttecs\Murmur\Tests\Support;

use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Server\Event;

/**
 * Record events published by the application.
 */
class FakePublisher implements Publisher
{
    /**
     * The events published in the order they were received.
     *
     * @var list<Event>
     */
    public array $events = [];

    /**
     * Record the given event.
     */
    public function publish(Event $event): void
    {
        $this->events[] = $event;
    }
}
