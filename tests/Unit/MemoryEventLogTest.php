<?php

use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\EventIdGenerator;
use Arttecs\Murmur\Server\Hub;
use Arttecs\Murmur\Server\Log\MemoryEventLog;

use function React\Async\await;

function logAt(int $capacity = 10000, int $start = 1000): MemoryEventLog
{
    $ms = $start;

    return new MemoryEventLog($capacity, new EventIdGenerator(function () use (&$ms) {
        return $ms++;
    }));
}

it('notifies later memory listeners and reports the error without interrupting delivery', function () {
    \React\Async\delay(1.01);
    $errorLog = tempnam(sys_get_temp_dir(), 'murmur');
    $previous = ini_set('error_log', $errorLog);

    try {
        $log = logAt();
        $seen = [];
        $log->subscribe(fn () => throw new RuntimeException('The first memory listener failed.'));
        $log->subscribe(function (Event $event) use (&$seen): void {
            $seen[] = $event;
        });

        $stored = $log->append(new Event('news', 'updated'));

        expect($seen)->toBe([$stored]);
        expect(file_get_contents($errorLog))->toContain('The first memory listener failed.');
    } finally {
        ini_set('error_log', (string) $previous);
        unlink($errorLog);
    }
});

it('reports a gap for a cursor from before the log was created', function () {
    $log = logAt(start: 5000);
    expect(await($log->since(new EventId(10, 0), ['news'], 1000))->gap)->toBeTrue();
});

it('reports no gap for a cursor at the last evicted event', function () {
    $log = logAt(capacity: 1);
    $first = $log->append(new Event('news', 'a'));
    $second = $log->append(new Event('news', 'b'));
    $replay = await($log->since($first->id, ['news'], 1000));
    expect($replay->gap)->toBeFalse();
    expect($replay->events)->toHaveCount(1);
});

it('replays in order after the buffer wraps around', function () {
    $log = logAt(capacity: 2);
    $first = $log->append(new Event('news', 'a'));
    $second = $log->append(new Event('news', 'b'));
    $third = $log->append(new Event('news', 'c'));

    $replay = await($log->since($first->id, ['news'], 1000));

    expect($replay->gap)->toBeFalse();
    expect(array_map(fn (Event $e) => (string) $e->id, $replay->events))->toBe([(string) $second->id, (string) $third->id]);
});

it('drops listeners and keeps the history when closed', function () {
    $log = logAt();
    $calls = 0;
    $log->subscribe(function () use (&$calls) {
        $calls++;
    });
    $log->close();
    $stored = $log->append(new Event('news', 'a'));

    expect($calls)->toBe(0);
    expect(await($log->since(new EventId(1000, 0), ['news'], 1000))->events)->toBe([$stored]);
});

it('rejects a capacity below one', function (int $capacity) {
    new MemoryEventLog($capacity);
})->with([0, -1])->throws(InvalidArgumentException::class);

it('limits delivery error logs to one line per second and counts suppressed errors', function () {
    React\Async\delay(1.01);
    $errorLog = tempnam(sys_get_temp_dir(), 'murmur');
    $previous = ini_set('error_log', $errorLog);

    try {
        $log = logAt();
        $log->subscribe(fn () => throw new RuntimeException("First failure\nwith a second line.", 0, new LogicException('Nested secret')));
        $log->append(new Event('news', 'first', 'Private payload'));

        $hub = new Hub;
        [$broken, , $stream] = fakeConnection();
        $stream->on('data', fn () => throw new RuntimeException('Suppressed hub error.'));
        $hub->attach($broken, ['news']);
        $hub->dispatch(new Event('news', 'second'));
        $log->append(new Event('news', 'third'));

        $lines = file($errorLog, FILE_IGNORE_NEW_LINES);
        expect($lines)->toHaveCount(1);
        expect($lines[0])->toContain('RuntimeException: First failure with a second line.')
            ->not->toContain('Stack trace', 'Nested secret', 'Private payload');

        React\Async\delay(1.01);
        $next = logAt();
        $next->subscribe(fn () => throw new LogicException('A later failure.'));
        $next->append(new Event('news', 'fourth'));

        $lines = file($errorLog, FILE_IGNORE_NEW_LINES);
        expect($lines)->toHaveCount(2);
        expect($lines[1])->toContain('LogicException: A later failure.', '2 errors suppressed');
    } finally {
        ini_set('error_log', (string) $previous);
        unlink($errorLog);
    }
});

it('merges interleaved topics and deduplicates subscriptions', function () {
    $log = logAt();
    $first = $log->append(new Event('b', 'first'));
    $second = $log->append(new Event('a', 'second'));
    $third = $log->append(new Event('b', 'third'));

    expect(await($log->since(new EventId(1000, 0), ['a', 'b', 'a'], 3))->events)
        ->toBe([$first, $second, $third]);
});

it('matches a brute-force replay across repeated wraps', function () {
    $log = logAt(capacity: 5);
    $history = [];

    for ($i = 0; $i < 300; $i++) {
        $history[] = $log->append(new Event(['a', 'b', 'c'][$i % 3], 'updated'));
        $after = $history[max(0, $i - ($i % 8))]->id;
        $expected = array_values(array_filter(array_slice($history, -5), fn (Event $event) => $event->topic !== 'b' && $event->id->compare($after) > 0));

        expect(await($log->since($after, ['c', 'a'], 5))->events)->toBe($expected);
    }
});

it('drops the index entry for a fully evicted topic', function () {
    $log = logAt(capacity: 1);
    $evicted = $log->append(new Event('old', 'updated'));
    $log->append(new Event('new', 'updated'));

    expect(await($log->since($evicted->id, ['old'], 5))->events)->toBe([]);
    expect((new ReflectionProperty($log, 'topicPositions'))->getValue($log))->not->toHaveKey('old');
});

it('counts only filtered events toward replay overflow', function () {
    $log = logAt();
    $first = $log->append(new Event('a', 'updated'));
    $second = $log->append(new Event('b', 'updated'));
    $third = $log->append(new Event('a', 'updated'));
    $after = new EventId(1000, 0);
    $filter = fn (Event $event) => $event !== $second;

    $overflow = await($log->since($after, ['a', 'b'], 1, $filter));

    expect($overflow->gap)->toBeTrue()
        ->and($overflow->events)->toBe([])
        ->and($overflow->tail)->toBe($third->id)
        ->and(await($log->since($after, ['a', 'b'], 2, $filter))->events)->toBe([$first, $third]);
});

it('finds a cursor between event sequences in the same millisecond', function () {
    $log = new MemoryEventLog(ids: new EventIdGenerator(fn () => 1000));
    $log->append(new Event('a', 'updated'));
    $between = $log->append(new Event('b', 'updated'));
    $later = $log->append(new Event('a', 'updated'));

    expect(await($log->since($between->id, ['a'], 2))->events)->toBe([$later]);
});
