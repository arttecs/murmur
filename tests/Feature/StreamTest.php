<?php

use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\Server\Auth\TokenVerifier;
use Illuminate\Support\Carbon;

it('emits stream events and an end marker', function () {
    $publisher = fakePublisher();
    $stream = Murmur::stream('private-chat.42');
    $stream->emit('token', ['text' => 'Olá']);
    $stream->close();
    expect($publisher->events)->toHaveCount(2);
    expect($publisher->events[0]->name)->toBe('token');
    expect($publisher->events[1]->name)->toBe('murmur.end');
});

it('builds a private stream url with a scoped token and a start cursor', function () {
    Carbon::setTestNow(Carbon::createFromTimestampMs(1732360005000));
    parse_str(parse_url(Murmur::stream('private-chat.42')->url(), PHP_URL_QUERY), $q);
    expect($q['topics'])->toBe('private-chat.42');
    expect($q['lastEventId'])->toBe('1732360004000-0');
    expect((new TokenVerifier('test-secret-with-at-least-32-bytes', fn () => now()->timestamp))->verify($q['token'])->topics)->toBe(['private-chat.42']);
});

it('builds a public stream url without a token', function () {
    parse_str(parse_url(Murmur::stream('progress.9')->url(), PHP_URL_QUERY), $q);
    expect($q)->not->toHaveKey('token');
});

it('honours a custom ttl', function () {
    Carbon::setTestNow('2026-01-01 00:00:00');
    parse_str(parse_url(Murmur::stream('private-x')->url(ttl: 3600), PHP_URL_QUERY), $q);
    expect((new TokenVerifier('test-secret-with-at-least-32-bytes', fn () => now()->timestamp))->verify($q['token'])->expiresAt)->toBe(now()->timestamp + 3600);
});
