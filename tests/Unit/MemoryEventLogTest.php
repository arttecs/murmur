<?php

use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\EventIdGenerator;
use Arttecs\Murmur\Server\Log\MemoryEventLog;

use function React\Async\await;

function logAt(int $capacity = 10000, int $start = 1000): MemoryEventLog
{
    $ms = $start;

    return new MemoryEventLog($capacity, new EventIdGenerator(function () use (&$ms) {
        return $ms++;
    }));
}

it('assigns ids and notifies subscribers', function () {
    $log = logAt();
    $seen = [];
    $log->subscribe(function (Event $e) use (&$seen) {
        $seen[] = (string) $e->id;
    });
    $stored = $log->append(new Event('news', 'a'));
    expect($seen)->toBe([(string) $stored->id]);
});

it('replays only later events of the requested topics', function () {
    $log = logAt();
    $first = $log->append(new Event('news', 'a'));
    $log->append(new Event('other', 'b'));
    $third = $log->append(new Event('news', 'c'));
    $replay = await($log->since($first->id, ['news']));
    expect($replay->gap)->toBeFalse();
    expect(array_map(fn (Event $e) => (string) $e->id, $replay->events))->toBe([(string) $third->id]);
});

it('reports a gap when the cursor is older than the evicted history', function () {
    $log = logAt(capacity: 2);
    $first = $log->append(new Event('news', 'a'));
    $log->append(new Event('news', 'b'));
    $log->append(new Event('news', 'c'));
    $log->append(new Event('news', 'd'));
    expect(await($log->since($first->id, ['news']))->gap)->toBeTrue();
});

it('reports a gap for a cursor from before the log was created', function () {
    $log = logAt(start: 5000);
    expect(await($log->since(new EventId(10, 0), ['news']))->gap)->toBeTrue();
});

it('reports no gap for a cursor at the last evicted event', function () {
    $log = logAt(capacity: 1);
    $first = $log->append(new Event('news', 'a'));
    $second = $log->append(new Event('news', 'b'));
    $replay = await($log->since($first->id, ['news']));
    expect($replay->gap)->toBeFalse();
    expect($replay->events)->toHaveCount(1);
});
