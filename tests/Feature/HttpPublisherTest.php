<?php

use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Exceptions\PublishFailed;
use Arttecs\Murmur\Server\Auth\Signature;
use Arttecs\Murmur\Server\Event;
use Illuminate\Http\Client\ConnectionException;
use Illuminate\Http\Client\Request;
use Illuminate\Support\Facades\Http;

it('posts a signed event to the server', function () {
    Http::fake(['*/publish' => Http::response(['id' => '1-0'], 202)]);
    app(Publisher::class)->publish(new Event('news', 'updated', ['id' => 7]));
    Http::assertSent(function (Request $request) {
        $ts = (int) $request->header('X-Murmur-Timestamp')[0];

        return $request->url() === 'http://127.0.0.1:8080/publish'
            && $request->data() === ['topic' => 'news', 'event' => 'updated', 'data' => ['id' => 7]]
            && Signature::verify('test-secret-with-at-least-32-bytes', $ts, $request->body(), $request->header('X-Murmur-Signature')[0], time());
    });
});

it('fails fast when the server is unreachable', function () {
    Http::fake(fn () => throw new ConnectionException('refused'));
    app(Publisher::class)->publish(new Event('news', 'x'));
})->throws(PublishFailed::class);

it('fails when the server rejects the event', function () {
    Http::fake(['*' => Http::response('', 401)]);
    app(Publisher::class)->publish(new Event('news', 'x'));
})->throws(PublishFailed::class);
