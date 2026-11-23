<?php

use Arttecs\Murmur\Server\Auth\TokenVerifier;
use Arttecs\Murmur\Tests\Fixtures\User;
use Illuminate\Support\Facades\Broadcast;

beforeEach(function () {
    config([
        'app.key' => 'base64:'.base64_encode(str_repeat('a', 32)),
        'broadcasting.default' => 'murmur',
        'broadcasting.connections.murmur' => ['driver' => 'murmur'],
    ]);
});

it('issues one token for all authorized channels', function () {
    Broadcast::channel('orders.{id}', fn ($user, $id) => (int) $id === 1);
    $response = $this->actingAs(new User(['id' => 9]))
        ->postJson('/murmur/auth', ['channels' => ['private-orders.1']])
        ->assertOk();
    $grant = (new TokenVerifier('test-secret-with-at-least-32-bytes'))->verify($response->json('token'));
    expect($grant->topics)->toBe(['private-orders.1']);
    expect($response->json('expires_at'))->toBe($grant->expiresAt);
});

it('denies the whole request when one channel is refused', function () {
    Broadcast::channel('orders.{id}', fn ($user, $id) => (int) $id === 1);
    $this->actingAs(new User(['id' => 9]))
        ->postJson('/murmur/auth', ['channels' => ['private-orders.1', 'private-orders.2']])
        ->assertForbidden();
});

it('denies guests', function () {
    Broadcast::channel('orders.{id}', fn () => true);
    $this->postJson('/murmur/auth', ['channels' => ['private-orders.1']])->assertForbidden();
});

it('validates the channel list', function (array $channels) {
    $this->actingAs(new User(['id' => 9]))
        ->postJson('/murmur/auth', ['channels' => $channels])
        ->assertStatus(422);
})->with([[[]], [['news']], [['presence-room.1']]]);
