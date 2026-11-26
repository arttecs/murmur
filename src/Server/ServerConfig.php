<?php

namespace Arttecs\Murmur\Server;

use Arttecs\Murmur\Server\Auth\Secret;
use InvalidArgumentException;
use SensitiveParameter;

/**
 * Hold the settings for the Murmur HTTP server.
 */
readonly class ServerConfig
{
    /**
     * Create the server configuration and validate its settings.
     *
     * @param  list<string>  $allowedOrigins
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        #[SensitiveParameter] public string $secret,
        public string $host = '127.0.0.1',
        public int $port = 8080,
        public float $heartbeatSeconds = 20.0,
        public int $retryMs = 3000,
        public int $maxConnections = 900,
        public int $maxTopicsPerConnection = 50,
        public int $maxPayloadBytes = 65536,
        public int $maxBufferBytes = 1048576,
        public array $allowedOrigins = ['*'],
        public bool $publishEnabled = true,
        public int $signatureToleranceSeconds = 60,
        public int $maxSockets = 1000,
        public float $requestTimeoutSeconds = 10.0,
        public int $maxReplayEvents = 1000,
        public float $shutdownGraceSeconds = 5.0,
        public bool $metricsEnabled = true,
        #[SensitiveParameter] public ?string $metricsToken = null,
    ) {
        Secret::assertValid($secret);

        if ($port < 1 || $port > 65535) {
            throw new InvalidArgumentException('The port must be between 1 and 65535.');
        }

        foreach (compact(
            'heartbeatSeconds',
            'retryMs',
            'maxConnections',
            'maxSockets',
            'maxTopicsPerConnection',
            'maxPayloadBytes',
            'maxBufferBytes',
            'maxReplayEvents',
            'requestTimeoutSeconds',
            'signatureToleranceSeconds',
            'shutdownGraceSeconds',
        ) as $setting => $value) {
            if ($value <= 0 || ! is_finite((float) $value)) {
                throw new InvalidArgumentException("The {$setting} setting must be positive and finite.");
            }
        }

        if ($metricsToken !== null && strlen($metricsToken) < 16) {
            throw new InvalidArgumentException('The metricsToken setting must be at least 16 bytes long.');
        }

        if ($maxSockets < $maxConnections) {
            throw new InvalidArgumentException('The maxSockets setting must be at least maxConnections.');
        }
    }
}
