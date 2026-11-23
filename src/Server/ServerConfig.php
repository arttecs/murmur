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
     * Create the server configuration and validate its shared secret.
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
        public int $maxConnections = 10000,
        public int $maxTopicsPerConnection = 50,
        public int $maxPayloadBytes = 65536,
        public int $maxBufferBytes = 1048576,
        public array $allowedOrigins = ['*'],
        public bool $publishEnabled = true,
        public int $signatureToleranceSeconds = 60,
    ) {
        Secret::assertValid($secret);
    }
}
