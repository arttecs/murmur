<?php

namespace Arttecs\Murmur\Server;

use Arttecs\Murmur\Server\Auth\Secret;
use Arttecs\Murmur\Server\Http\Origin;
use InvalidArgumentException;
use SensitiveParameter;

/**
 * Hold the settings for the Murmur HTTP server.
 */
readonly class ServerConfig
{
    /**
     * The normalized origins allowed to open a subscription.
     *
     * @var list<string>
     */
    public array $allowedOrigins;

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
        array $allowedOrigins = [],
        public bool $publishEnabled = true,
        public int $signatureToleranceSeconds = 60,
        public int $maxSockets = 1000,
        public float $requestTimeoutSeconds = 10.0,
        public int $maxReplayEvents = 1000,
        public float $shutdownGraceSeconds = 5.0,
        public int $loopLagThresholdMs = 100,
        public float $shutdownSpreadSeconds = 2.0,
        public int $backlog = 4096,
        public int $maxTopicsPerPublish = 100,
    ) {
        Secret::assertValid($secret);

        $origins = [];

        foreach ($allowedOrigins as $origin) {
            $normalized = $origin === '*' ? '*' : (is_string($origin) ? Origin::normalize($origin) : null);

            if ($normalized === null) {
                throw new InvalidArgumentException('The allowedOrigins setting must contain only * or valid origins without a path, query, or fragment.');
            }

            $origins[] = $normalized;
        }

        $this->allowedOrigins = $origins;

        if ($port < 1 || $port > 65535) {
            throw new InvalidArgumentException('The port must be between 1 and 65535.');
        }

        foreach (compact(
            'heartbeatSeconds',
            'backlog',
            'retryMs',
            'maxConnections',
            'maxSockets',
            'maxTopicsPerConnection',
            'maxTopicsPerPublish',
            'maxPayloadBytes',
            'maxBufferBytes',
            'maxReplayEvents',
            'requestTimeoutSeconds',
            'signatureToleranceSeconds',
            'shutdownGraceSeconds',
            'loopLagThresholdMs',
        ) as $setting => $value) {
            if ($value <= 0 || ! is_finite((float) $value)) {
                throw new InvalidArgumentException("The {$setting} setting must be positive and finite.");
            }
        }

        if (! is_finite($shutdownSpreadSeconds) || $shutdownSpreadSeconds < 0 || $shutdownSpreadSeconds >= $shutdownGraceSeconds) {
            throw new InvalidArgumentException('The shutdownSpreadSeconds setting must be non-negative and less than shutdownGraceSeconds.');
        }

        if ($backlog > 65535) {
            throw new InvalidArgumentException('The backlog setting must not exceed 65535.');
        }

        if ($maxSockets < $maxConnections) {
            throw new InvalidArgumentException('The maxSockets setting must be at least maxConnections.');
        }
    }
}
