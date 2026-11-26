<?php

namespace Arttecs\Murmur\Server\Auth;

use InvalidArgumentException;
use SensitiveParameter;

/**
 * Validate the shared Murmur secret.
 *
 * @internal
 */
class Secret
{
    /**
     * Ensure that the given secret meets the minimum byte length.
     *
     * @throws InvalidArgumentException
     */
    public static function assertValid(#[SensitiveParameter] string $secret): void
    {
        if (strlen($secret) < 32) {
            throw new InvalidArgumentException('The Murmur secret must be at least 32 bytes long.');
        }
    }
}
