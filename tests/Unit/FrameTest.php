<?php

use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Sse\Frame;

it('rejects event names with line breaks', function () {
    new Event('news', "bad\nname");
})->throws(InvalidArgumentException::class);

it('rejects event names with a trailing line break', function () {
    new Event('news', "bad\n");
})->throws(InvalidArgumentException::class);

it('encodes an event frame', function () {
    $event = new Event('private-orders.1', 'OrderShipped', ['id' => 1], new EventId(1732360000000, 4));
    expect(Frame::event($event))->toBe(
        "id: 1732360000000-4\nevent: OrderShipped\ndata: {\"topic\":\"private-orders.1\",\"data\":{\"id\":1}}\n\n"
    );
});

it('keeps multi-line payloads on a single data line', function () {
    $frame = Frame::event(new Event('news', 'note', ['text' => "a\nb"], new EventId(1, 0)));
    expect(substr_count($frame, "\n"))->toBe(4);
});

it('encodes control frames', function () {
    expect(Frame::comment())->toBe(": ping\n\n");
    expect(Frame::retry(3000))->toBe("retry: 3000\n\n");
    expect(Frame::gap())->toBe("event: murmur.gap\ndata: {}\n\n");
});
