<?php

namespace Arttecs\Murmur\Server\Auth;

/**
 * Hold the topics and expiration time of a verified subscription token.
 *
 * @internal
 */
class Grant
{
    /**
     * Create a new subscription grant.
     *
     * @param  list<string>  $topics
     */
    public function __construct(public readonly array $topics, public readonly int $expiresAt) {}

    /**
     * Determine if the grant allows the given topic.
     */
    public function allows(string $topic): bool
    {
        return in_array($topic, $this->topics, true);
    }
}
