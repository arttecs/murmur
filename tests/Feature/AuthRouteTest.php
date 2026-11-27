<?php

use Arttecs\Murmur\MurmurServiceProvider;
use Arttecs\Murmur\Server\Auth\TokenVerifier;
use Arttecs\Murmur\Tests\Fixtures\User;
use Illuminate\Support\Facades\Auth;
use Illuminate\Support\Facades\Broadcast;
use Illuminate\Support\Facades\Route;

beforeEach(function () {
    config([
        'app.key' => 'base64:'.base64_encode(str_repeat('a', 32)),
        'broadcasting.default' => 'murmur',
    ]);
    Broadcast::routes();
});

it('issues a token scoped to the channel requested through the laravel route', function () {
    Broadcast::channel('orders.{id}', fn (User $user, string $id): bool => $user->id === 9 && $id === '1');

    $response = $this->actingAs(new User(['id' => 9]))
        ->postJson('/broadcasting/auth', ['channel_name' => 'private-orders.1', 'socket_id' => 'any legacy socket'])
        ->assertOk()
        ->assertJsonStructure(['token', 'expires_at']);

    $grant = (new TokenVerifier(config('murmur.secret')))->verify($response->json('token'));
    expect($grant->topics)->toBe(['private-orders.1']);
    expect($response->json('expires_at'))->toBe($grant->expiresAt);
});

it('denies a private channel when the callback refuses it', function () {
    Broadcast::channel('orders.{id}', fn (): bool => false);

    $this->actingAs(new User(['id' => 9]))
        ->postJson('/broadcasting/auth', ['channel_name' => 'private-orders.1'])
        ->assertForbidden();
});

it('denies guests on private channels', function () {
    Broadcast::channel('orders.{id}', fn (): bool => true);

    $this->postJson('/broadcasting/auth', ['channel_name' => 'private-orders.1'])->assertForbidden();
});

it('denies public and malformed channel names before any callback runs', function (array $input) {
    foreach (['news', 'private-', 'private-orders 1', 'private-orders.1,private-admin', 'private-'.str_repeat('x', 200)] as $channel) {
        Broadcast::channel($channel, fn () => $this->fail('The channel callback ran.'));
    }

    $this->actingAs(new User(['id' => 9]))
        ->postJson('/broadcasting/auth', $input)
        ->assertForbidden();
})->with([
    'missing' => [[]],
    'array' => [['channel_name' => ['private-orders.1']]],
    'public' => [['channel_name' => 'news']],
    'empty channel' => [['channel_name' => 'private-']],
    'space' => [['channel_name' => 'private-orders 1']],
    'comma' => [['channel_name' => 'private-orders.1,private-admin']],
    'too long' => [['channel_name' => 'private-'.str_repeat('x', 200)]],
]);

it('denies presence channels with an explanation', function () {
    Broadcast::channel('rooms.1', fn (): bool => true);

    $this->actingAs(new User(['id' => 9]))
        ->postJson('/broadcasting/auth', ['channel_name' => 'presence-rooms.1'])
        ->assertForbidden()
        ->assertJsonPath('message', 'Murmur does not support presence channels.');
});

it('uses the configured guard for each channel', function () {
    config([
        'auth.guards.partner' => ['driver' => 'murmur-partner'],
        'auth.guards.staff' => ['driver' => 'murmur-staff'],
    ]);
    Auth::viaRequest('murmur-partner', fn () => new User(['id' => 9]));
    Auth::viaRequest('murmur-staff', fn () => null);
    Broadcast::channel('orders.{id}', fn (User $user): bool => $user->id === 9, ['guards' => ['partner']]);
    Broadcast::channel('staff.{id}', fn (): bool => true, ['guards' => ['staff']]);

    $this->postJson('/broadcasting/auth', ['channel_name' => 'private-orders.1'])->assertOk();
    $this->actingAs(new User(['id' => 9]))
        ->postJson('/broadcasting/auth', ['channel_name' => 'private-staff.1'])
        ->assertForbidden();
});

it('registers no package authorization route even with legacy configuration', function () {
    config(['murmur.auth' => ['enabled' => true, 'path' => 'old/auth', 'middleware' => []]]);
    $provider = new MurmurServiceProvider($this->app);
    $provider->register();
    $provider->boot();

    expect(Route::has('murmur.auth'))->toBeFalse();
    $this->postJson('/murmur/auth')->assertNotFound();
    $this->postJson('/old/auth')->assertNotFound();
});

it('authorizes only well-formed UTF-8 private names within the byte limit', function (string $topic, bool $valid) {
    $topic = strlen($topic) >= 199 ? substr($topic, strlen('private-')) : $topic;

    Broadcast::channel($topic, fn (): bool => true);
    $response = $this->actingAs(new User(['id' => 9]))
        ->post('/broadcasting/auth', ['channel_name' => 'private-'.$topic])
        ->assertStatus($valid ? 200 : 403);

    if ($valid) {
        expect((new TokenVerifier(config('murmur.secret')))->verify($response->json('token'))->topics)
            ->toBe(['private-'.$topic]);
    }
})->with('wire topic boundaries');
