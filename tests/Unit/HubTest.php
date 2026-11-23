<?php

use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Hub;

it('delivers an event only to connections subscribed to its topic', function () {
    $hub = new Hub;
    [$a, $outA] = fakeConnection();
    [$b, $outB] = fakeConnection();
    $hub->attach($a, ['news']);
    $hub->attach($b, ['other']);
    $hub->dispatch(new Event('news', 'x', null, new EventId(1, 0)));
    expect($outA())->toContain('event: x');
    expect($outB())->toBe('');
});

it('writes a single frame to a connection subscribed to several topics', function () {
    $hub = new Hub;
    [$a, $out] = fakeConnection();
    $hub->attach($a, ['news', 'other']);
    $hub->dispatch(new Event('news', 'x', null, new EventId(1, 0)));
    expect(substr_count($out(), 'event: x'))->toBe(1);
});

it('forgets a connection when its stream closes', function () {
    $hub = new Hub;
    [$a, , $stream] = fakeConnection();
    $hub->attach($a, ['news']);
    $stream->close();
    expect($hub->connectionCount())->toBe(0);
});

it('sends heartbeats to every connection', function () {
    $hub = new Hub;
    [$a, $out] = fakeConnection();
    $hub->attach($a, ['news']);
    $hub->heartbeat();
    expect($out())->toBe(": ping\n\n");
});

it('stops delivering to a detached connection', function () {
    $hub = new Hub;
    [$a, $out] = fakeConnection();
    $hub->attach($a, ['news']);
    $hub->detach($a);
    $hub->dispatch(new Event('news', 'x', null, new EventId(1, 0)));

    expect($out())->toBe('');
    expect($hub->connectionCount())->toBe(0);
    expect($a->isClosed())->toBeFalse();
});

it('replaces the topics of a connection that attaches again', function () {
    $hub = new Hub;
    [$a, $out] = fakeConnection();
    $hub->attach($a, ['news']);
    $hub->attach($a, ['other']);
    $hub->dispatch(new Event('news', 'x', null, new EventId(1, 0)));
    $hub->dispatch(new Event('other', 'y', null, new EventId(2, 0)));

    expect($out())->not->toContain('event: x');
    expect($out())->toContain('event: y');
    expect($hub->connectionCount())->toBe(1);
});

it('closes every connection', function () {
    $hub = new Hub;
    [$a] = fakeConnection();
    [$b] = fakeConnection();
    $hub->attach($a, ['news']);
    $hub->attach($b, ['other']);
    $hub->closeAll();

    expect($a->isClosed())->toBeTrue();
    expect($b->isClosed())->toBeTrue();
    expect($hub->connectionCount())->toBe(0);
});
