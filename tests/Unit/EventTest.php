<?php

use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;

it('preserves the assigned identifier and event when copying an exclusion', function () {
    $id = new EventId(1, 0);
    $event = new Event('news', 'updated', 7, $id);
    $except = str_repeat('a', 32);
    $copy = $event->withExcept($except);
    $newId = new EventId(2, 0);

    expect($copy->withId($newId))->toEqual(new Event('news', 'updated', 7, $newId, $except));
    expect($event->except)->toBeNull();
    expect($copy->withExcept(null))->toEqual($event);
    expect(fn () => $copy->withExcept('bad'))->toThrow(InvalidArgumentException::class);
});
