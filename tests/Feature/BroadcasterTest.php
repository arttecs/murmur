<?php

use Arttecs\Murmur\Exceptions\PresenceNotSupported;
use Arttecs\Murmur\Tests\Fixtures\NewsUpdated;
use Arttecs\Murmur\Tests\Fixtures\OrderShipped;
use Arttecs\Murmur\Tests\Fixtures\SomeoneJoined;

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
