<?php

use Arttecs\Murmur\Server\Auth\TokenVerifier;
use Arttecs\Murmur\Tests\Fixtures\User;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Auth;
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
})->with([
    'empty' => [[]],
    'public' => [['news']],
    'presence' => [['presence-room.1']],
    'comma' => [['private-orders.1,private-admin']],
    'inner space' => [['private-orders 1']],
    'too long' => [['private-'.str_repeat('x', 200)]],
    'too many' => [array_map(fn (int $i) => "private-orders.{$i}", range(1, 51))],
]);

it('validates an invalid list before authorizing a guest', function () {
    $this->postJson('/murmur/auth', ['channels' => []])->assertUnprocessable();
});

it('issues one token covering several channels', function () {
    Broadcast::channel('orders.{id}', fn ($user, $id) => true);
    $response = $this->actingAs(new User(['id' => 9]))
        ->postJson('/murmur/auth', ['channels' => ['private-orders.1', 'private-orders.2']])
        ->assertOk();

    expect((new TokenVerifier('test-secret-with-at-least-32-bytes'))->verify($response->json('token'))->topics)
        ->toBe(['private-orders.1', 'private-orders.2']);
});

it('authorizes a single channel through the broadcaster auth method', function () {
    Broadcast::channel('orders.{id}', fn ($user, $id) => (int) $id === 1);
    $request = Request::create('/broadcasting/auth', 'POST', ['channel_name' => 'private-orders.1']);
    $request->setUserResolver(fn () => new User(['id' => 9]));

    $result = Broadcast::connection('murmur')->auth($request);

    expect((new TokenVerifier('test-secret-with-at-least-32-bytes'))->verify($result['token'])->topics)->toBe(['private-orders.1']);
});

it('requires the channel guard even when the default guard has a user', function () {
    config(['auth.guards.api' => ['driver' => 'murmur-request']]);
    Auth::viaRequest('murmur-request', fn () => null);
    Broadcast::channel('orders.{id}', fn () => true, ['guards' => ['api']]);

    $this->actingAs(new User(['id' => 9]))
        ->postJson('/murmur/auth', ['channels' => ['private-orders.1']])
        ->assertForbidden();
});

it('authorizes with the channel guard when the default guard has no user', function () {
    config(['auth.guards.api' => ['driver' => 'murmur-request']]);
    Auth::viaRequest('murmur-request', fn () => new User(['id' => 9]));
    Broadcast::channel('orders.{id}', fn (User $user) => $user->id === 9, ['guards' => ['api']]);

    $this->postJson('/murmur/auth', ['channels' => ['private-orders.1']])->assertOk();
});

it('limits the total channel name size in bytes', function () {
    Broadcast::channel('orders.{id}', fn () => true);
    $channels = array_map(fn (int $i) => "private-orders.{$i}".str_repeat('x', 180 - strlen((string) $i)), range(1, 10));
    $channels[] = 'private-orders.'.str_repeat("\u{00E9}", 41).'x';
    expect(array_sum(array_map(strlen(...), $channels)))->toBe(2048);

    $this->actingAs(new User(['id' => 9]));
    $this->postJson('/murmur/auth', ['channels' => $channels])->assertOk();

    $channels[10] .= 'x';
    $this->postJson('/murmur/auth', ['channels' => $channels])
        ->assertUnprocessable()
        ->assertJsonValidationErrors('channels');
});
