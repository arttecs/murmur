<?php

use Arttecs\Murmur\Server\ServerConfig;

it('rejects an invalid server setting and names it in the error', function (string $setting, int|float $value) {
    expect(fn () => new ServerConfig('test-secret-with-at-least-32-bytes', ...[$setting => $value]))
        ->toThrow(InvalidArgumentException::class, $setting);
})->with([
    ['port', 0],
    ['port', 65536],
    ['heartbeatSeconds', 0.0],
    ['heartbeatSeconds', INF],
    ['retryMs', -1],
    ['maxConnections', 0],
    ['maxSockets', 0],
    ['maxSockets', 899],
    ['maxTopicsPerConnection', 0],
    ['maxPayloadBytes', 0],
    ['maxBufferBytes', 0],
    ['maxReplayEvents', 0],
    ['requestTimeoutSeconds', 0.0],
    ['requestTimeoutSeconds', NAN],
    ['signatureToleranceSeconds', 0],
    ['shutdownGraceSeconds', 0.0],
    ['shutdownGraceSeconds', INF],
]);
