<?php

use Arttecs\Murmur\Server\ServerConfig;

it('rejects an invalid server setting and names it in the error', function (string $setting, int|float $value) {
    expect(fn () => new ServerConfig('test-secret-with-at-least-32-bytes', ...[$setting => $value]))
        ->toThrow(InvalidArgumentException::class, $setting);
})->with([
    ['port', 0],
    ['port', 65536],
    ['heartbeatSeconds', 0.0],
    ['loopLagThresholdMs', 0],
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
    ['shutdownSpreadSeconds', -0.1],
    ['shutdownSpreadSeconds', 5.0],
    ['shutdownSpreadSeconds', 6.0],
    ['shutdownSpreadSeconds', NAN],
    ['shutdownSpreadSeconds', INF],
]);

it('normalizes explicit default ports to browser origins while retaining non-default ports', function () {
    $config = new ServerConfig(str_repeat('x', 32), allowedOrigins: [
        'HTTPS://App.test:443',
        'http://app.test:80',
        'http://app.test:00443',
        'https://[0:0:0:0:0:0:0:1]:0443',
    ]);

    expect($config->allowedOrigins)->toBe([
        'https://app.test',
        'http://app.test',
        'http://app.test:443',
        'https://[::1]',
    ]);
});
