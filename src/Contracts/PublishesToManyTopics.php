<?php

namespace Arttecs\Murmur\Contracts;

use Arttecs\Murmur\Exceptions\PublishFailed;

/**
 * Allow a publisher to send an event to several topics.
 */
interface PublishesToManyTopics
{
    /**
     * Publish one event to each topic in order, all-or-nothing where the transport allows it.
     *
     * @param  list<string>  $topics
     *
     * @throws PublishFailed
     */
    public function publishToTopics(array $topics, string $name, mixed $data = null, ?string $except = null): void;
}
