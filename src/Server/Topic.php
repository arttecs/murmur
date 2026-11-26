<?php

namespace Arttecs\Murmur\Server;

/**
 * Validate and classify topic names.
 *
 * @internal
 */
class Topic
{
    /**
     * Determine if the given topic name is valid.
     */
    public static function isValid(string $name): bool
    {
        return preg_match('/\A[^\s,]{1,200}\z/', $name) === 1;
    }

    /**
     * Determine if the given topic requires a subscription token.
     */
    public static function isPrivate(string $name): bool
    {
        return str_starts_with($name, 'private-');
    }
}
