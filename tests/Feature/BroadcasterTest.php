<?php

use Arttecs\Murmur\Exceptions\PresenceNotSupported;
use Arttecs\Murmur\Tests\Fixtures\NewsUpdated;
use Arttecs\Murmur\Tests\Fixtures\OrderShipped;
use Arttecs\Murmur\Tests\Fixtures\SomeoneJoined;
use Illuminate\Support\Facades\Broadcast;

beforeEach(function () {
    config(['broadcasting.default' => 'murmur', 'broadcasting.connections.murmur' => ['driver' => 'murmur']]);
});

it('publishes a broadcast event to each channel', function () {
    $publisher = fakePublisher();
    broadcast(new OrderShipped(orderId: 1));
    expect($publisher->events)->toHaveCount(1);
    expect($publisher->events[0]->topic)->toBe('private-orders.1');
    expect($publisher->events[0]->name)->toBe(OrderShipped::class);
    expect($publisher->events[0]->data)->toBe(['orderId' => 1]);
});

it('uses broadcastAs when defined', function () {
    $publisher = fakePublisher();
    broadcast(new NewsUpdated);
    expect($publisher->events[0]->topic)->toBe('news');
    expect($publisher->events[0]->name)->toBe('updated');
});

it('refuses presence channels', function () {
    fakePublisher();
    broadcast(new SomeoneJoined);
})->throws(PresenceNotSupported::class);

it('removes the socket id from the payload', function () {
    $publisher = fakePublisher();
    Broadcast::connection('murmur')->broadcast(['news'], 'updated', ['id' => 7, 'socket' => 'abc.123']);

    expect($publisher->events[0]->data)->toBe(['id' => 7]);
});

it('publishes nothing when one of the channels is a presence channel', function () {
    $publisher = fakePublisher();

    expect(fn () => Broadcast::connection('murmur')->broadcast(['news', 'presence-room.1'], 'updated'))
        ->toThrow(PresenceNotSupported::class);
    expect($publisher->events)->toBe([]);
});

it('rejects reserved event names when broadcasting', function () {
    $publisher = fakePublisher();

    expect(fn () => Broadcast::connection('murmur')->broadcast(['news'], 'murmur.end'))
        ->toThrow(InvalidArgumentException::class, 'Event names starting with "murmur." are reserved.');
    expect($publisher->events)->toBe([]);
});

it('validates broadcast topic UTF-8 and byte length', function (string $topic, bool $valid) {
    $publisher = fakePublisher();
    $broadcast = fn () => Broadcast::connection('murmur')->broadcast([$topic], 'updated');

    if (! $valid) {
        expect($broadcast)->toThrow(InvalidArgumentException::class)
            ->and($publisher->events)->toBe([]);

        return;
    }

    $broadcast();
    expect($publisher->events[0]->topic)->toBe($topic);
})->with('wire topic boundaries');
