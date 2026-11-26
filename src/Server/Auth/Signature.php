<?php

namespace Arttecs\Murmur\Server\Auth;

use InvalidArgumentException;
use SensitiveParameter;

/**
 * Sign and verify timestamped publish requests.
 *
 * @internal
 */
class Signature
{
    /**
     * Sign the given timestamp and request body.
     *
     * @throws InvalidArgumentException
     */
    public static function sign(#[SensitiveParameter] string $secret, int $timestamp, string $body): string
    {
        Secret::assertValid($secret);

        return hash_hmac('sha256', "{$timestamp}.{$body}", $secret);
    }

    /**
     * Verify the signature and timestamp of a publish request.
     *
     * @throws InvalidArgumentException
     */
    public static function verify(#[SensitiveParameter] string $secret, int $timestamp, string $body, string $signature, int $now, int $toleranceSeconds = 60): bool
    {
        $expected = self::sign($secret, $timestamp, $body);
        $age = abs($now - $timestamp);

        return is_int($age) && $age <= $toleranceSeconds && hash_equals($expected, $signature);
    }
}
