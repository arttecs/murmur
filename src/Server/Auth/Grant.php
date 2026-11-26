<?php

namespace Arttecs\Murmur\Server\Auth;

/**
 * Hold the granted topics and Unix expiration time of a subscription credential.
 */
readonly class Grant
{
    /**
     * Create a new subscription grant.
     *
     * @param  list<string>  $topics
     */
    public function __construct(public array $topics, public int $expiresAt) {}

    /**
     * Determine if the grant allows the given topic.
     */
    public function allows(string $topic): bool
    {
        return in_array($topic, $this->topics, true);
    }
}
