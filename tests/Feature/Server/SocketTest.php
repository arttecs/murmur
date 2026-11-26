<?php

use Arttecs\Murmur\Server\Auth\Signature;
use Arttecs\Murmur\Server\Event;
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

it('finishes a signed publish already in flight when shutdown starts', function () {
    $secret = 'test-secret-with-at-least-32-bytes';
    $h = ServerHarness::start(['secret' => $secret]);
    $client = $h->connect();
    $headers = new Deferred;
    $finished = new Deferred;
    $response = '';

    $client->on('data', function (string $data) use (&$response, $headers): void {
        $response .= $data;

        if (str_contains($response, "100 Continue\r\n\r\n")) {
            $headers->resolve(null);
        }
    });
    $client->once('close', fn () => $finished->resolve(null));
    $body = '{"topic":"news","event":"finished"}';
    $timestamp = time();
    $signature = Signature::sign($secret, $timestamp, $body);
    $length = strlen($body);

    try {
        $client->write("POST /publish HTTP/1.1\r\nHost: localhost\r\nExpect: 100-continue\r\nContent-Length: {$length}\r\nX-Murmur-Timestamp: {$timestamp}\r\nX-Murmur-Signature: {$signature}\r\n\r\n");
        await(timeout($headers->promise(), 1.0));

        $shutdown = $h->shutdown(1.0);
        expect($h->trackedSocketCount())->toBe(1);
        $client->write($body);
        await(timeout($finished->promise(), 2.0));
        await(timeout($shutdown, 1.0));

        expect($response)->toContain('202 Accepted');
        expect($h->trackedSocketCount())->toBe(0);
    } finally {
        $client->close();
    }
});

it('flushes sse output and closes unfinished sockets when the shutdown grace ends', function (?float $graceSeconds) {
    $h = ServerHarness::start(['shutdownGraceSeconds' => 0.15]);
    $sse = $h->connect();
    $idle = $h->connect();
    $retry = new Deferred;
    $flushed = new Deferred;
    $idleClosed = new Deferred;
    $response = '';

    $sse->on('data', function (string $data) use (&$response, $retry): void {
        $response .= $data;

        if (str_contains($response, 'retry: 3000')) {
            $retry->resolve(null);
        }
    });
    $sse->once('close', fn () => $flushed->resolve(null));
    $idle->once('close', fn () => $idleClosed->resolve(null));

    try {
        $sse->write("GET /sse?topics=news HTTP/1.1\r\nHost: localhost\r\n\r\n");
        await(timeout($retry->promise(), 1.0));
        $h->log->append(new Event('news', 'pending'));
        $shutdown = $h->shutdown($graceSeconds);

        await(timeout($flushed->promise(), 1.0));
        expect($response)->toContain('event: pending')->toEndWith("0\r\n\r\n");
        expect($h->trackedSocketCount())->toBe(1);
        await(timeout($idleClosed->promise(), 1.0));
        await(timeout($shutdown, 1.0));
        expect($h->trackedSocketCount())->toBe(0);
    } finally {
        $sse->close();
        $idle->close();
    }
})->with(['configured' => [null], 'explicit' => [0.15]]);
