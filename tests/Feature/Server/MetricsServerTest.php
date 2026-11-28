<?php

use Arttecs\Murmur\Server\ErrorReporter;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventIdGenerator;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SlowReplayLog;
use Arttecs\Murmur\Tests\Support\SseReader;
use React\Http\Browser;

use function React\Async\await;

/**
 * Read one server's samples keyed by metric name and, when present, HTTP status.
 *
 * @return array<string, int|float>
 */
function serverMetricValues(ServerHarness $h): array
{
    $values = [];

    foreach ($h->metrics()->snapshot() as $sample) {
        $name = $sample['name'].(isset($sample['labels']['status']) ? '.'.$sample['labels']['status'] : '');
        $values[$name] = $sample['value'];
    }

    return $values;
}

it('tracks events, connections, output bytes and gauges for a listening server', function () {
    $log = new MemoryEventLog(ids: new EventIdGenerator(fn (): int => 1000));
    $h = ServerHarness::start(log: $log);
    $initial = serverMetricValues($h);
    expect($initial)->toMatchArray([
        'murmur_connections_opened_total' => 0,
        'murmur_connections_closed_total' => 0,
        'murmur_slow_consumers_total' => 0,
        'murmur_events_received_total' => 0,
        'murmur_events_sent_total' => 0,
        'murmur_bytes_sent_total' => 0,
        'murmur_replays_total' => 0,
        'murmur_replay_failures_total' => 0,
        'murmur_replayed_events_total' => 0,
        'murmur_gaps_total' => 0,
        'murmur_errors_total' => 0,
        'murmur_connections' => 0,
        'murmur_sockets' => 0,
        'murmur_topics' => 0,
    ]);
    expect($initial['murmur_memory_bytes'])->toBeInt()->toBeGreaterThan(0);
    expect($initial['murmur_memory_peak_bytes'])->toBeGreaterThanOrEqual($initial['murmur_memory_bytes']);

    $id = str_repeat('a', 32);
    $connected = "event: murmur.connected\ndata: {\"connection_id\":\"{$id}\",\"protocol\":1}\n\n";
    $reader = SseReader::open($h->url('/sse?topics=news,weather&connection='.$id));
    expect($reader->start()['retry'])->toBe('3000');
    $open = serverMetricValues($h);
    expect($open)->toMatchArray([
        'murmur_connections_opened_total' => 1,
        'murmur_connections_closed_total' => 0,
        'murmur_bytes_sent_total' => strlen("retry: 3000\n\n{$connected}event: murmur.attached\ndata: {}\n\n"),
        'murmur_connections' => 1,
        'murmur_sockets' => 1,
        'murmur_topics' => 2,
        'murmur_replays_total' => 0,
        'murmur_events_sent_total' => 0,
    ]);
    expect($open['murmur_uptime_seconds'])->toBeGreaterThan($initial['murmur_uptime_seconds']);

    $log->append(new Event('other', 'ignored'));
    expect(serverMetricValues($h))->toMatchArray([
        'murmur_events_received_total' => 1,
        'murmur_events_sent_total' => 0,
        'murmur_bytes_sent_total' => $open['murmur_bytes_sent_total'],
    ]);

    $event = $log->append(new Event('news', 'updated'));
    expect($reader->next()['event'])->toBe('updated');
    expect(serverMetricValues($h))->toMatchArray([
        'murmur_events_received_total' => 2,
        'murmur_events_sent_total' => 1,
        'murmur_replayed_events_total' => 0,
        'murmur_bytes_sent_total' => strlen("retry: 3000\n\n{$connected}event: murmur.attached\ndata: {}\n\nid: {$event->id}\nevent: updated\ndata: {\"topic\":\"news\",\"data\":null}\n\n"),
    ]);

    $reader->close();
    $h->pollHealth(0);
    expect(serverMetricValues($h))->toMatchArray([
        'murmur_connections_opened_total' => 1,
        'murmur_connections_closed_total' => 1,
        'murmur_slow_consumers_total' => 0,
        'murmur_connections' => 0,
        'murmur_sockets' => 0,
        'murmur_topics' => 0,
    ]);
});

it('counts SSE rejection statuses without opening or closing a stream for them', function () {
    $h = ServerHarness::start(['maxConnections' => 1]);
    $token = $h->token(['private-other']);
    SseReader::open($h->url('/sse?topics=news'))->next();

    foreach (['' => 400, '?topics=private-news' => 401, '?topics=private-news&token='.$token => 403, '?topics=news' => 503] as $query => $status) {
        expect(SseReader::open($h->url('/sse'.$query))->status())->toBe($status);
    }
    expect(SseReader::open($h->url('/sse?topics=private-news'))->status())->toBe(401);

    $values = serverMetricValues($h);
    expect(array_filter($values, fn (string $name): bool => str_starts_with($name, 'murmur_connections_rejected_total'), ARRAY_FILTER_USE_KEY))->toBe([
        'murmur_connections_rejected_total.400' => 1,
        'murmur_connections_rejected_total.401' => 2,
        'murmur_connections_rejected_total.403' => 1,
        'murmur_connections_rejected_total.503' => 1,
    ]);
    expect($values)->toMatchArray(['murmur_connections_opened_total' => 1, 'murmur_connections_closed_total' => 0]);
});

it('counts the bytes in a heartbeat without treating it as an event', function (bool $numbered) {
    $log = new MemoryEventLog(ids: new EventIdGenerator(fn (): int => 1000));
    $h = ServerHarness::start(['heartbeatSeconds' => 0.1], $log);
    $id = str_repeat('a', 32);
    $connected = "event: murmur.connected\ndata: {\"connection_id\":\"{$id}\",\"protocol\":1}\n\n";
    $reader = SseReader::open($h->url('/sse?topics=news&connection='.$id));
    expect($reader->start()['retry'])->toBe('3000');

    $heartbeat = ': ping';

    if ($numbered) {
        $log->append(new Event('other', 'unrelated'));
        $heartbeat = "id: 1000-1\nevent: murmur.heartbeat\ndata: {}";
    }

    expect($reader->nextRaw())->toBe($heartbeat);

    expect(serverMetricValues($h))->toMatchArray([
        'murmur_bytes_sent_total' => strlen("retry: 3000\n\n{$connected}event: murmur.attached\ndata: {}\n\n{$heartbeat}\n\n"),
        'murmur_events_sent_total' => 0,
        'murmur_gaps_total' => 0,
    ]);
})->with([false, true]);

it('counts replayed events and gap frames at the time they are written', function () {
    $log = new MemoryEventLog(ids: new EventIdGenerator(fn (): int => 1000));
    $h = ServerHarness::start(log: $log);
    $event = $log->append(new Event('news', 'retained'));
    $id = str_repeat('a', 32);
    $connected = "event: murmur.connected\ndata: {\"connection_id\":\"{$id}\",\"protocol\":1}\n\n";
    $reader = SseReader::open($h->url('/sse?topics=news&connection='.$id), ['Last-Event-ID' => '1-0']);

    expect($reader->start(replaying: true)['retry'])->toBe('3000');
    expect($reader->next()['event'])->toBe('murmur.gap');
    expect($reader->next()['event'])->toBe('retained');
    expect($reader->attached()['id'])->toBe((string) $event->id);
    expect(serverMetricValues($h))->toMatchArray([
        'murmur_replays_total' => 1,
        'murmur_events_received_total' => 1,
        'murmur_events_sent_total' => 1,
        'murmur_replayed_events_total' => 1,
        'murmur_gaps_total' => 1,
        'murmur_bytes_sent_total' => strlen("retry: 3000\n\n{$connected}event: murmur.gap\ndata: {}\n\nid: {$event->id}\nevent: retained\ndata: {\"topic\":\"news\",\"data\":null}\n\nid: {$event->id}\nevent: murmur.attached\ndata: {}\n\n"),
    ]);
});

it('counts slow consumers only when their output or pending replay queue exceeds the limit', function (bool $held) {
    $log = $held ? new SlowReplayLog(3.0) : new MemoryEventLog;
    $h = ServerHarness::start(['maxBufferBytes' => 65536], $log);
    $client = $h->subscribeWithoutReading('/sse?topics=news'.($held ? '&lastEventId=1-0' : ''));

    try {
        $h->pollHealth(1);
        expect(serverMetricValues($h)['murmur_slow_consumers_total'])->toBe(0);

        $log->append(new Event('news', 'large', str_repeat('x', 131072)));
        $h->pollHealth(0);

        expect(serverMetricValues($h))->toMatchArray([
            'murmur_slow_consumers_total' => 1,
            'murmur_connections_closed_total' => 1,
            'murmur_events_received_total' => 1,
            'murmur_events_sent_total' => $held ? 0 : 1,
            'murmur_replayed_events_total' => 0,
            'murmur_replay_failures_total' => 0,
            'murmur_errors_total' => 0,
        ]);
    } finally {
        $client->close();
    }
})->with(['live backpressure' => [false], 'held queue' => [true]]);

it('counts publish responses by status and only accepted publishes reach the hub', function () {
    $h = ServerHarness::start(['maxPayloadBytes' => 64]);
    expect($h->publish('news', 'accepted')->getStatusCode())->toBe(202);
    expect($h->rawPublish('{"topics":["news","updates"],"event":"accepted"}')->getStatusCode())->toBe(202);
    expect($h->rawPublish('{}', signature: 'bad')->getStatusCode())->toBe(401);
    expect($h->rawPublish(str_repeat('x', 65))->getStatusCode())->toBe(413);
    expect($h->rawPublish('{}')->getStatusCode())->toBe(422);

    expect(serverMetricValues($h))->toMatchArray([
        'murmur_publish_requests_total.202' => 2,
        'murmur_publish_requests_total.401' => 1,
        'murmur_publish_requests_total.413' => 1,
        'murmur_publish_requests_total.422' => 1,
        'murmur_events_received_total' => 3,
        'murmur_events_sent_total' => 0,
    ]);
});

it('counts a publish to a disabled endpoint as a 404', function () {
    $h = ServerHarness::start(['publishEnabled' => false]);
    expect($h->publish('news', 'disabled')->getStatusCode())->toBe(404);
    expect(serverMetricValues($h))->toMatchArray([
        'murmur_publish_requests_total.404' => 1,
        'murmur_events_received_total' => 0,
    ]);
});

it('counts reported errors even if rate limited and clears the hook when closed', function () {
    $errorLog = tempnam(sys_get_temp_dir(), 'murmur');
    $previous = ini_set('error_log', $errorLog);
    $h = ServerHarness::start();

    try {
        ErrorReporter::report(new RuntimeException('one'));
        ErrorReporter::report(new RuntimeException('two'));
        expect(serverMetricValues($h)['murmur_errors_total'])->toBe(2);

        $h->stop();
        ErrorReporter::report(new RuntimeException('outside server'));
        expect(serverMetricValues($h)['murmur_errors_total'])->toBe(2);
    } finally {
        ini_set('error_log', (string) $previous);
        unlink($errorLog);
    }
});

it('serves no metrics route', function () {
    $h = ServerHarness::start();
    $response = await((new Browser)->withRejectErrorResponse(false)->get($h->url('/metrics'), ['Accept' => 'application/json']));

    expect($response->getStatusCode())->toBe(404);
    expect((string) $response->getBody())->toBe('');
});
