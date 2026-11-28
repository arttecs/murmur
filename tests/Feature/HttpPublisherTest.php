<?php

use Arttecs\Murmur\Broadcasting\MurmurBroadcaster;
use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Exceptions\PublishFailed;
use Arttecs\Murmur\MurmurManager;
use Arttecs\Murmur\MurmurServiceProvider;
use Arttecs\Murmur\Server\Auth\Signature;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Tests\Support\FakePublisher;
use GuzzleHttp\Exception\ResponseTransferException;
use GuzzleHttp\Psr7\Request as PsrRequest;
use GuzzleHttp\Psr7\Response as PsrResponse;
use Illuminate\Http\Client\ConnectionException;
use Illuminate\Http\Client\Request;
use Illuminate\Support\Carbon;
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

it('batches broadcasts and sends one request per topic to an older server until the downgrade expires', function () {
    Carbon::setTestNow('2026-01-01 00:00:00');
    Http::fakeSequence()->pushStatus(202)->pushStatus(422)->whenEmpty(Http::response('', 202));
    $manager = app(MurmurManager::class);
    $broadcaster = new MurmurBroadcaster($manager);
    $broadcaster->broadcast(['news', 'updates', 'news'], 'updated');
    $manager->publishToTopics(['news', 'updates'], 'updated');
    $manager->publishToTopics(['news', 'updates'], 'updated');
    Carbon::setTestNow('2026-01-01 00:01:00');
    $manager->publishToTopics(['news', 'updates'], 'updated');

    expect(Http::recorded()->map(fn ($pair) => $pair[0]->data())->all())->toBe([
        ['topics' => ['news', 'updates'], 'event' => 'updated', 'data' => []],
        ['topics' => ['news', 'updates'], 'event' => 'updated', 'data' => null],
        ['topic' => 'news', 'event' => 'updated', 'data' => null],
        ['topic' => 'updates', 'event' => 'updated', 'data' => null],
        ['topic' => 'news', 'event' => 'updated', 'data' => null],
        ['topic' => 'updates', 'event' => 'updated', 'data' => null],
        ['topics' => ['news', 'updates'], 'event' => 'updated', 'data' => null],
    ]);
});

it('keeps the downgrade when an older server also rejects a topic', function () {
    Carbon::setTestNow('2026-01-01 00:00:00');
    Http::fakeSequence()->pushStatus(422)->pushStatus(202)->pushStatus(500)->whenEmpty(Http::response('', 202));
    $manager = app(MurmurManager::class);

    expect(fn () => $manager->publishToTopics(['news', 'updates'], 'updated'))->toThrow(PublishFailed::class, '500');
    $manager->publishToTopics(['news', 'updates'], 'updated');

    expect(Http::recorded()->map(fn ($pair) => array_key_exists('topics', $pair[0]->data()))->all())->toBe([true, false, false, false, false]);
});

it('splits a list longer than the configured topic limit', function () {
    config(['murmur.server.max_topics_per_publish' => 2]);
    Http::fake(['*' => Http::response('', 202)]);

    app(MurmurManager::class)->publishToTopics(['a', 'b', 'c', 'd', 'e'], 'updated');

    expect(Http::recorded()->map(fn ($pair) => $pair[0]->data()['topics'] ?? $pair[0]->data()['topic'])->all())->toBe([['a', 'b'], ['c', 'd'], 'e']);
});

it('splits a batch by the lower limit the server advertises and remembers it', function () {
    Http::fake(fn (Request $request) => count($request->data()['topics'] ?? []) > 2
        ? Http::response('', 422, ['X-Murmur-Publish-Topics' => '2'])
        : Http::response('', 202));
    $manager = app(MurmurManager::class);

    $manager->publishToTopics(['a', 'b', 'c'], 'updated');
    $manager->publishToTopics(['a', 'b', 'c'], 'updated');

    expect(Http::recorded()->map(fn ($pair) => $pair[0]->data()['topics'] ?? $pair[0]->data()['topic'])->all())->toBe([
        ['a', 'b', 'c'], ['a', 'b'], 'c',
        ['a', 'b'], 'c',
    ]);
});

it('halves a batch that exceeds the payload limit until it fits', function () {
    Http::fake(fn (Request $request) => count($request->data()['topics'] ?? []) > 2
        ? Http::response('', 413)
        : Http::response('', 202));

    app(MurmurManager::class)->publishToTopics(['a', 'b', 'c', 'd', 'e'], 'updated');

    expect(Http::recorded()->map(fn ($pair) => $pair[0]->data()['topics'] ?? $pair[0]->data()['topic'])->all())->toBe([
        ['a', 'b', 'c', 'd', 'e'], ['a', 'b', 'c'], ['a', 'b'], 'c', ['d', 'e'],
    ]);
});

it('fails when a single topic exceeds the payload limit', function () {
    Http::fake(['*' => Http::response('', 413)]);

    expect(fn () => app(MurmurManager::class)->publishToTopics(['a', 'b'], 'updated'))->toThrow(PublishFailed::class, '413');
    Http::assertSentCount(2);
});

it('validates a batch before falling back to a custom publisher', function () {
    $publisher = new FakePublisher;
    app()->instance(Publisher::class, $publisher);
    $manager = app(MurmurManager::class);

    expect(fn () => $manager->publishToTopics(['news', 'bad topic'], 'updated'))->toThrow(InvalidArgumentException::class);
    expect($publisher->events)->toBe([]);

    $manager->publishToTopics(['updates', 'news'], 'updated');

    expect(array_column($publisher->events, 'topic'))->toBe(['updates', 'news']);
});

it('uses the original request shape for a one-topic publish', function () {
    Http::fake(['*' => Http::response('', 202)]);

    app(MurmurManager::class)->publishToTopics(['news'], 'updated');

    Http::assertSentCount(1);
    Http::assertSent(fn (Request $request) => $request->data() === ['topic' => 'news', 'event' => 'updated', 'data' => null]);
});

it('does not retry a batch that a server supporting topics rejected', function () {
    Http::fake(['*' => Http::response('', 422, ['X-Murmur-Publish-Topics' => '100'])]);
    $manager = app(MurmurManager::class);

    expect(fn () => $manager->publishToTopics(['news', 'updates', 'alerts'], 'updated'))->toThrow(PublishFailed::class, '422');
    expect(fn () => $manager->publishToTopics(['news', 'updates'], 'updated'))->toThrow(PublishFailed::class, '422');

    expect(Http::recorded()->map(fn ($pair) => array_key_exists('topics', $pair[0]->data()))->all())->toBe([true, true]);
});
