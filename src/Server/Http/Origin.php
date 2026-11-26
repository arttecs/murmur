<?php

namespace Arttecs\Murmur\Server\Http;

/**
 * Parse serialized browser origins for SSE subscriptions.
 *
 * @internal
 */
class Origin
{
    /**
     * Validate an origin and normalize its scheme, host, and default HTTP port.
     */
    public static function normalize(string $origin): ?string
    {
        $pattern = '/\A([a-z][a-z0-9+.-]*):\/\/(\[[a-f0-9:.]+\]|[a-z0-9_-]+(?:\.[a-z0-9_-]+)*\.?)(?::([0-9]+))?\z/i';

        if (preg_match($pattern, $origin, $parts) !== 1
            || (isset($parts[3]) && (int) $parts[3] > 65535)
            || ($parts[2][0] === '[' && filter_var(substr($parts[2], 1, -1), FILTER_VALIDATE_IP, FILTER_FLAG_IPV6) === false)) {
            return null;
        }

        $scheme = strtolower($parts[1]);
        $port = $parts[3] ?? null;
        $defaultPort = match ($scheme) {
            'http' => 80,
            'https' => 443,
            default => null,
        };

        return $scheme.'://'.strtolower($parts[2]).($port === null || (int) $port === $defaultPort ? '' : ':'.$port);
    }
}
