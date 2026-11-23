<?php

use Arttecs\Murmur\Server\Topic;

it('validates topics', function (string $name, bool $valid) {
    expect(Topic::isValid($name))->toBe($valid);
})->with([['orders.1', true], ['private-chat.42', true], ['', false], ['a,b', false], ['a b', false], ["a\nb", false], ["a\n", false], [str_repeat('x', 201), false]]);

it('detects private topics', function () {
    expect(Topic::isPrivate('private-orders.1'))->toBeTrue();
    expect(Topic::isPrivate('orders.1'))->toBeFalse();
});
