<?php

use Arttecs\Murmur\Server\Auth\Grant;

it('rejects grants that do not contain a list of valid topic names', function (array $topics) {
    expect(fn () => new Grant($topics, PHP_INT_MAX))->toThrow(InvalidArgumentException::class);
})->with([
    'associative' => [['topic' => 'private-news']],
    'sparse' => [[1 => 'private-news']],
    'non-string' => [[42]],
    'empty name' => [['']],
    'space' => [['private news']],
    'comma' => [['private-a,private-b']],
    'trailing newline' => [["private-news\n"]],
    'long name' => [[str_repeat('a', 201)]],
]);

it('rejects nonpositive expiry timestamps', function (int $expiry) {
    expect(fn () => new Grant([], $expiry))->toThrow(InvalidArgumentException::class);
})->with([0, -1]);
