<?php

namespace Arttecs\Murmur\Server\Auth;

use Arttecs\Murmur\Server\Topic;
use InvalidArgumentException;

/**
 * Hold the granted topics and Unix expiration time of a subscription credential.
 */
readonly class Grant
{
    /**
     * Create a new subscription grant.
     *
     * @param  list<string>  $topics
     *
     * @throws InvalidArgumentException
     */
    public function __construct(public array $topics, public int $expiresAt)
    {
        if (! array_is_list($topics) || array_filter($topics, fn (mixed $topic): bool => is_string($topic) && Topic::isValid($topic)) !== $topics) {
            throw new InvalidArgumentException('A subscription grant must contain a list of valid topic names.');
        }

        if ($expiresAt <= 0) {
            throw new InvalidArgumentException('A subscription grant must have a positive Unix expiration timestamp.');
        }
    }

    /**
     * Determine if the grant allows the given topic.
     */
    public function allows(string $topic): bool
    {
        return in_array($topic, $this->topics, true);
    }
}
