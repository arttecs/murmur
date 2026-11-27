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
     * Validate an origin and canonicalize its scheme, host, and port.
     */
    public static function normalize(string $origin): ?string
    {
        $pattern = '/\A([A-Za-z][A-Za-z0-9+.-]*):\/\/(\[[A-Fa-f0-9:.]+\]|[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*\.?)(?::([0-9]+))?\z/';

        if (preg_match($pattern, $origin, $parts) !== 1) {
            return null;
        }

        $scheme = strtolower($parts[1]);
        $host = strtolower($parts[2]);
        $port = isset($parts[3]) ? (int) $parts[3] : null;

        if ($port !== null && $port > 65535) {
            return null;
        }

        if ($host[0] === '[') {
            $address = inet_pton(substr($host, 1, -1));

            if ($address === false || strlen($address) !== 16) {
                return null;
            }

            $host = '['.inet_ntop($address).']';
        }

        $defaultPort = match ($scheme) {
            'http' => 80,
            'https' => 443,
            default => null,
        };

        return $scheme.'://'.$host.($port === null || $port === $defaultPort ? '' : ':'.$port);
    }
}
