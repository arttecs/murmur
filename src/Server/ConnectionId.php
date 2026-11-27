<?php

namespace Arttecs\Murmur\Server;

/**
 * Validate identifiers used for sender exclusion.
 *
 * @internal
 */
class ConnectionId
{
    /**
     * Determine whether the value is a well-formed connection identifier.
     */
    public static function isValid(string $value): bool
    {
        return preg_match('/\A[0-9a-f]{32}\z/', $value) === 1;
    }
}
