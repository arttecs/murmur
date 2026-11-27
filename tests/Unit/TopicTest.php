<?php

use Arttecs\Murmur\Server\Auth\Grant;
use Arttecs\Murmur\Server\Topic;

it('validates topics', function (string $name, bool $valid) {
    expect(Topic::isValid($name))->toBe($valid);
})->with([['orders.1', true], ['private-chat.42', true], ['', false], ['a,b', false], ['a b', false], ["a\nb", false], ["a\n", false], [str_repeat('x', 201), false]]);

it('detects private topics', function () {
    expect(Topic::isPrivate('private-orders.1'))->toBeTrue();
    expect(Topic::isPrivate('orders.1'))->toBeFalse();
});

it('requires well-formed UTF-8 within the byte limit', function (string $topic, bool $valid) {
    expect(Topic::isValid($topic))->toBe($valid);
})->with('wire topic boundaries');

it('applies the topic matrix under installed non-C locales', function (array $locales) {
    $previous = setlocale(LC_CTYPE, '0');

    if (setlocale(LC_CTYPE, ...$locales) === false) {
        $this->markTestSkipped('None of the candidate locales are installed.');
    }

    try {
        foreach (datasetWireTopics() as $case => [$topic, $valid]) {
            expect(Topic::isValid($topic))->toBe($valid, $case);
        }

        expect(Topic::isValid('a'))->toBeTrue()
            ->and(Topic::isValid(''))->toBeFalse()
            ->and(Topic::isValid('a,b'))->toBeFalse();
    } finally {
        setlocale(LC_CTYPE, $previous);
    }
})->with([
    'Brazilian Portuguese or C.UTF-8' => [['pt_BR.UTF-8', 'pt_BR.utf8', 'C.UTF-8', 'C.utf8']],
    'US English' => [['en_US.UTF-8', 'en_US.utf8']],
]);

it('validates topics in subscription grants', function (string $topic, bool $valid) {
    if ($valid) {
        expect((new Grant([$topic], PHP_INT_MAX))->topics)->toBe([$topic]);
    } else {
        expect(fn () => new Grant([$topic], PHP_INT_MAX))->toThrow(InvalidArgumentException::class);
    }
})->with('wire topic boundaries');
