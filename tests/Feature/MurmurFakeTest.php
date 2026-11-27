<?php

use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\Server\Auth\TokenVerifier;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Testing\MurmurFake;
use Arttecs\Murmur\Tests\Fixtures\NewsUpdated;
use Arttecs\Murmur\Tests\Fixtures\OrderShipped;
use Arttecs\Murmur\Tests\Fixtures\User;
use Illuminate\Support\Facades\Broadcast;
use PHPUnit\Framework\AssertionFailedError;

it('records direct publications in order and accepts publication assertions', function () {
    $fake = Murmur::fake();
    expect($fake)->toBeInstanceOf(MurmurFake::class)->toBe(app(Publisher::class));
    $fake->assertNothingPublished();
    $fake->assertNotPublished('news');
    $fake->assertPublishedTimes('news', 'updated', 0);

    Murmur::publish('news', 'updated', ['id' => 1]);
    Murmur::publish('weather', 'updated', ['id' => 2]);
    Murmur::publish('news', 'updated', ['id' => 3]);
    Murmur::publish('news', 'removed');

    $fake->assertPublished('news');
    $fake->assertPublished('news', 'updated');
    $fake->assertPublished('news', 'updated', fn (Event $event): bool => $event->data === ['id' => 3]);
    $fake->assertPublishedTimes('news', 'updated', 2);
    $fake->assertNotPublished('news', 'created');
    $fake->assertNotPublished('unrelated');
    expect(array_map(fn (Event $event): array => [$event->topic, $event->name, $event->data], $fake->published()))->toBe([
        ['news', 'updated', ['id' => 1]],
        ['weather', 'updated', ['id' => 2]],
        ['news', 'updated', ['id' => 3]],
        ['news', 'removed', null],
    ]);

    $next = Murmur::fake();
    $next->assertNothingPublished();
    $fake->assertPublishedTimes('news', 'updated', 2);
});

it('fails with the expected publication details when assertions are unmet', function () {
    $fake = Murmur::fake();
    Murmur::publish('news', 'updated', 1);

    expect(fn () => $fake->assertPublished('weather'))->toThrow(AssertionFailedError::class, 'weather');
    expect(fn () => $fake->assertPublished('news', 'removed'))->toThrow(AssertionFailedError::class, 'removed');
    expect(fn () => $fake->assertPublished('news', 'updated', fn (Event $event): bool => $event->data === 9))
        ->toThrow(AssertionFailedError::class, 'updated');
    expect(fn () => $fake->assertPublishedTimes('news', 'updated', 2))->toThrow(AssertionFailedError::class, '2');
    expect(fn () => $fake->assertNotPublished('news', 'updated'))->toThrow(AssertionFailedError::class, 'updated');
    expect(fn () => $fake->assertNotPublished('news'))->toThrow(AssertionFailedError::class, 'news');
    expect(fn () => $fake->assertNothingPublished())->toThrow(AssertionFailedError::class, 'no events');
});

it('records streams and issues private tokens with no configured secret while faked', function () {
    config(['murmur.secret' => null]);
    expect(fn () => Murmur::token(['private-chat']))->toThrow(InvalidArgumentException::class);

    $fake = Murmur::fake();
    $stream = Murmur::stream('private-chat');
    $stream->emit('token', ['text' => 'Hello']);
    $stream->close();
    parse_str(parse_url($stream->url(), PHP_URL_QUERY), $parameters);

    expect($parameters['topics'])->toBe('private-chat');
    expect($parameters['token'])->toBeString()->not->toBeEmpty();
    expect(config('murmur.secret'))->toBeNull();
    $fake->assertPublished('private-chat', 'token', fn (Event $event): bool => $event->data === ['text' => 'Hello']);
    $fake->assertPublished('private-chat', 'murmur.end');
});

it('keeps using the configured secret and captures broadcasts through an already resolved connection', function () {
    config([
        'app.key' => 'base64:'.base64_encode(str_repeat('a', 32)),
        'broadcasting.default' => 'murmur',
    ]);
    Broadcast::routes();
    Broadcast::channel('orders.{orderId}', fn (User $user, string $orderId) => $orderId === '42');

    $fake = Murmur::fake();
    event(new NewsUpdated);
    event(new OrderShipped(42, 'socket'));

    $fake->assertPublished('news', 'updated');
    $fake->assertPublished('private-orders.42', OrderShipped::class, fn (Event $event): bool => $event->data === ['orderId' => 42]);
    $token = $this->actingAs(new User(['id' => 9]))
        ->postJson('/broadcasting/auth', ['channel_name' => 'private-orders.42'])
        ->assertOk()
        ->json('token');
    $grant = (new TokenVerifier(config('murmur.secret')))->verify($token);
    expect($grant->topics)->toBe(['private-orders.42']);
});

it('records a direct publication with its excluded connection and rejects invalid identifiers', function () {
    $fake = Murmur::fake();
    $id = str_repeat('b', 32);
    Murmur::publish('news', 'updated', ['id' => 9], except: $id);

    $fake->assertPublished('news', 'updated', fn (Event $event): bool => $event->except === $id);
    expect(fn () => Murmur::publish('news', 'updated', except: $id."\n"))
        ->toThrow(InvalidArgumentException::class);
    $fake->assertPublishedTimes('news', 'updated', 1);
});
