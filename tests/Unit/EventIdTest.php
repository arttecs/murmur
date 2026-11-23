<?php

use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\EventIdGenerator;

it('parses and prints ids', function () {
    expect((string) EventId::tryParse('1732360000000-3'))->toBe('1732360000000-3');
    expect(EventId::tryParse('abc'))->toBeNull();
    expect(EventId::tryParse('12-'))->toBeNull();
    expect(EventId::tryParse(''))->toBeNull();
});

it('orders ids by ms then seq', function () {
    expect((new EventId(5, 1))->compare(new EventId(5, 2)))->toBeLessThan(0);
    expect((new EventId(6, 0))->compare(new EventId(5, 9)))->toBeGreaterThan(0);
    expect((new EventId(5, 1))->compare(new EventId(5, 1)))->toBe(0);
});

it('generates strictly increasing ids even when the clock stalls or goes back', function () {
    $times = [100, 100, 99, 101];
    $gen = new EventIdGenerator(function () use (&$times) {
        return array_shift($times);
    });
    expect(array_map(fn () => (string) $gen->next(), range(1, 4)))
        ->toBe(['100-0', '100-1', '100-2', '101-0']);
});
