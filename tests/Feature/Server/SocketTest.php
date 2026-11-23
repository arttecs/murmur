<?php

use Arttecs\Murmur\Tests\Support\ServerHarness;
use React\Promise\Deferred;

use function React\Async\await;
use function React\Promise\Timer\timeout;

it('closes a socket that does not finish its request before the deadline', function (string $request) {
    $h = ServerHarness::start(['requestTimeoutSeconds' => 0.1]);
    $client = $h->connect();
    $closed = new Deferred;
    $client->once('close', fn () => $closed->resolve(null));

    try {
        if ($request !== '') {
            $client->write($request);
        }

        await(timeout($closed->promise(), 1.0));
        expect($h->trackedSocketCount())->toBe(0);
    } finally {
        $client->close();
    }
})->with([
    'idle' => [''],
    'missing publish body' => ["POST /publish HTTP/1.1\r\nHost: localhost\r\nContent-Length: 20\r\nX-Murmur-Timestamp: 123\r\nX-Murmur-Signature: test\r\n\r\n"],
]);

it('immediately closes a socket above the limit and serves the accepted socket', function () {
    $h = ServerHarness::start(['maxSockets' => 1, 'maxConnections' => 1]);
    $first = $h->connect();
    $extra = $h->connect();
    $closed = new Deferred;
    $extra->once('close', fn () => $closed->resolve(null));

    try {
        await(timeout($closed->promise(), 0.5));
        expect($h->trackedSocketCount())->toBe(1);

        $response = '';
        $finished = new Deferred;
        $first->on('data', function (string $data) use (&$response): void {
            $response .= $data;
        });
        $first->once('close', fn () => $finished->resolve(null));
        $first->write("GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n");

        await(timeout($finished->promise(), 1.0));
        expect($response)->toContain('200 OK', "Connection: close\r\n", '{"status":"ok","connections":0}');
    } finally {
        $first->close();
        $extra->close();
    }
});
