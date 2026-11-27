<?php

use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Exceptions\PublishFailed;
use Arttecs\Murmur\MurmurManager;
use Arttecs\Murmur\MurmurServiceProvider;
use Arttecs\Murmur\Server\Auth\Signature;
use Arttecs\Murmur\Server\Event;
use GuzzleHttp\Exception\ResponseTransferException;
use GuzzleHttp\Psr7\Request as PsrRequest;
use GuzzleHttp\Psr7\Response as PsrResponse;
use Illuminate\Http\Client\ConnectionException;
use Illuminate\Http\Client\Request;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\ServiceProvider;

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

it('fails when the transfer breaks after an error status', function () {
    Http::fake(fn () => throw new ResponseTransferException('reset', new PsrRequest('POST', 'http://127.0.0.1:8080/publish'), new PsrResponse(500)));
    app(Publisher::class)->publish(new Event('news', 'x'));
})->skip(fn () => ! class_exists(ResponseTransferException::class), 'Requires Guzzle 8.')->throws(PublishFailed::class, 'The publish request to the Murmur server failed.');

it('boots without a secret and fails only when murmur is used', function (?string $secret) {
    config(['murmur.secret' => $secret]);

    expect(fn () => app(Publisher::class))->toThrow(InvalidArgumentException::class, 'at least 32 bytes');
    expect(fn () => app(MurmurManager::class)->publish('news', 'updated'))->toThrow(InvalidArgumentException::class, 'at least 32 bytes');
})->with([null, '', 'short']);

it('rejects an unsupported driver', function () {
    config(['murmur.driver' => 'carrier-pigeon']);
    app(Publisher::class);
})->throws(InvalidArgumentException::class, 'carrier-pigeon');

it('publishes its config file', function () {
    $paths = ServiceProvider::pathsToPublish(MurmurServiceProvider::class, 'murmur-config');

    expect(array_values($paths))->toBe([config_path('murmur.php')]);
    expect(realpath(array_key_first($paths)))->toBe(realpath(__DIR__.'/../../config/murmur.php'));
});

it('reports an unencodable http payload as a publish failure', function () {
    Http::fake();

    try {
        app(Publisher::class)->publish(new Event('news', 'updated', INF));
    } catch (PublishFailed $error) {
        expect($error->getPrevious())->toBeInstanceOf(JsonException::class);
        Http::assertNothingSent();

        return;
    }

    $this->fail('The publish did not fail.');
});
