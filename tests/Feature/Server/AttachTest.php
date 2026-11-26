<?php

use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\EventIdGenerator;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Server\Log\RedisEventLog;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SseReader;
use Clue\Redis\Protocol\Factory as ProtocolFactory;
use React\Promise\Deferred;
use React\Promise\PromiseInterface;
use React\Socket\ConnectionInterface;
use React\Socket\SocketServer;

use function React\Async\await;
use function React\Promise\Timer\timeout;

/**
 * Open the selected log and return a publisher that yields the assigned ID.
 */
function attachTestLog(string $driver, int $capacity = 10000): array
{
    if ($driver === 'redis') {
        [$log, $key] = redisLog(maxLength: $capacity);

        $client = test()->target->redisFixtures[$key]['client'];

        return [$log, function (string $topic, string $name, ?string $except = null) use ($client, $key, $capacity): string {
            $fields = $except === null ? [] : ['except', $except];

            return await($client->xadd($key, 'MAXLEN', '=', $capacity, '*', 'topic', $topic, 'name', $name, 'data', 'null', ...$fields));
        }];
    }

    $log = new MemoryEventLog($capacity);

    return [$log, fn (string $topic, string $name, ?string $except = null): string => (string) $log->append(new Event($topic, $name, except: $except))->id];
}

it('writes the literal handshake and replay before live delivery', function (string $driver, bool $cursor, bool $fresh, string $history) {
    [$log, $publish] = attachTestLog($driver);
    $h = ServerHarness::start(['maxReplayEvents' => 1], $log);
    $id = str_repeat('a', 32);
    $after = '0-0';
    $tail = null;
    $replayed = null;

    if ($history !== 'empty') {
        $after = $publish('old', 'seen');
        $replayed = $publish('old', 'replayed');

        if ($history === 'over limit') {
            $publish('old', 'over-limit');
        }

        $publish('old', 'excluded', $id);
        $tail = $publish($fresh ? 'new' : 'unsubscribed', 'tail');
    } else {
        $after = '999999999999999999-0';
    }

    $path = '/sse?topics=old,new&connection='.$id.($fresh ? '&fresh=new' : '');
    $r = SseReader::open($h->url($path), $cursor ? ['Last-Event-ID' => $after] : []);
    expect($r->nextRaw())->toBe('retry: 3000');
    expect($r->nextRaw())->toBe("event: murmur.connected\ndata: {\"connection_id\":\"{$id}\",\"protocol\":1}");

    if ($cursor && $history === 'over limit') {
        expect($r->nextRaw())->toBe("event: murmur.gap\ndata: {}");
    }

    if ($cursor && $history === 'within limit') {
        expect($r->nextRaw())->toBe("id: {$replayed}\nevent: replayed\ndata: {\"topic\":\"old\",\"data\":null}");
    }

    $expected = $tail === null ? '' : "id: {$tail}\n";
    expect($r->nextRaw())->toBe($expected."event: murmur.attached\ndata: {}");
    $live = $publish('new', 'live');
    expect($r->nextRaw())->toBe("id: {$live}\nevent: live\ndata: {\"topic\":\"new\",\"data\":null}");

    if ($cursor && $history === 'over limit') {
        $resumed = SseReader::open($h->url('/sse?topics=old,new&connection='.$id), ['Last-Event-ID' => $tail]);
        $resumed->next();
        $resumed->next();
        expect($resumed->next()['id'])->toBe($live);
        expect($resumed->attached()['id'])->toBe($live);
    }
})->with(['memory', 'redis'], [false, true], [false, true], ['empty', 'within limit', 'over limit']);

it('applies the cursor and fresh topics to events held before the tail is sampled', function (string $driver, bool $cursor, bool $fresh) {
    $sample = new Deferred;
    $read = new Deferred;
    $socket = $driver === 'redis' ? new SocketServer('127.0.0.1:0') : null;
    $topics = ['old', 'new', 'old'];

    if ($socket === null) {
        $log = new class($sample) extends MemoryEventLog
        {
            public function __construct(protected Deferred $sample)
            {
                parent::__construct(ids: new EventIdGenerator(fn (): int => 10));
                $this->append(new Event('old', 'history'));
            }

            public function since(EventId $after, array $topics, int $limit, ?callable $filter = null): PromiseInterface
            {
                return $this->sample->promise()->then(fn () => parent::since($after, $topics, $limit, $filter));
            }
        };
    } else {
        $log = new RedisEventLog(str_replace('tcp://', 'redis://', $socket->getAddress()), 'test');
        $factory = new ProtocolFactory;
        $serializer = $factory->createSerializer();
        $entries = [];

        foreach ($topics as $index => $topic) {
            $number = $index + 2;
            $entries[] = ["10-{$number}", ['topic', $topic, 'name', "raced-{$number}", 'data', 'null']];
        }

        $history = ['10-1', ['topic', 'old', 'name', 'history', 'data', 'null']];
        $starting = true;
        $socket->on('connection', function (ConnectionInterface $client) use ($factory, $serializer, $history, $entries, $sample, $read, &$starting): void {
            $parser = $factory->createRequestParser();
            $reader = $starting;
            $starting = false;

            $client->on('data', function (string $data) use ($parser, $client, $serializer, $history, $entries, $sample, $read, $reader): void {
                foreach ($parser->pushIncoming($data) as $message) {
                    $arguments = $message->getValueNative();

                    match (strtoupper($arguments[0])) {
                        'XREVRANGE' => $reader
                            ? $client->write($serializer->getReplyMessage([$history]))
                            : $sample->promise()->then(fn () => $client->write($serializer->getReplyMessage([$entries[2]]))),
                        'XREAD' => $arguments[7] === '10-1'
                            ? $read->promise()->then(fn () => $client->write($serializer->getReplyMessage([['test', $entries]])))
                            : null,
                        'XRANGE' => $client->write($serializer->getReplyMessage($entries)),
                        'XINFO' => $client->write($serializer->getReplyMessage(['first-entry', $history, 'entries-added', 4, 'length', 4])),
                    };
                }
            });
        });
    }

    try {
        $h = ServerHarness::start(log: $log);
        $dispatched = new Deferred;
        $log->subscribe(function (Event $event) use ($dispatched): void {
            if ($event->name === 'raced-4') {
                $dispatched->resolve(null);
            }
        });

        $path = '/sse?topics=old,new'.($fresh ? '&fresh=new' : '');
        $r = SseReader::open($h->url($path), $cursor ? ['Last-Event-ID' => '10-1'] : []);
        expect($r->start(replaying: true)['retry'])->toBe('3000');

        if ($socket === null) {
            foreach ($topics as $index => $topic) {
                $log->append(new Event($topic, 'raced-'.($index + 2)));
            }
        } else {
            $read->resolve(null);
        }

        await(timeout($dispatched->promise(), 1.0));
        $sample->resolve(null);

        foreach ($fresh ? [2, 4] : [2, 3, 4] as $number) {
            $topic = $topics[$number - 2];
            expect($r->nextRaw())->toBe("id: 10-{$number}\nevent: raced-{$number}\ndata: {\"topic\":\"{$topic}\",\"data\":null}");
        }

        expect($r->attached()['id'])->toBe('10-4');
        expect(fn () => $r->next(0.05))->toThrow(RuntimeException::class, 'Timed out');
    } finally {
        $log->close();
        $socket?->close();
    }
})->with(['memory', 'redis'], [false, true], [false, true]);

it('resumes a newly joined topic from the acknowledged join after disconnecting', function (string $driver) {
    [$log, $publish] = attachTestLog($driver);
    $h = ServerHarness::start(log: $log);
    $cursor = $publish('old', 'before-reconnect');
    $initial = SseReader::open($h->url('/sse?topics=old'));
    $initial->next();
    $initial->next();
    expect($initial->next()['id'])->toBe($cursor);
    $initial->close();
    $retained = $publish('old', 'retained');
    $tail = $publish('new', 'before-join');

    $joining = SseReader::open($h->url('/sse?topics=old,new&fresh=new'), ['Last-Event-ID' => $cursor]);
    $joining->next();
    $joining->next();
    expect($joining->next()['id'])->toBe($retained);
    $marker = $joining->next();
    expect($marker['event'])->toBe('murmur.attached')->and($marker['id'])->toBe($tail);
    $joining->close();

    $missedNew = $publish('new', 'missed-new');
    $missedOld = $publish('old', 'missed-old');
    $resumed = SseReader::open($h->url('/sse?topics=old,new'), ['Last-Event-ID' => $marker['id']]);
    $resumed->next();
    $resumed->next();
    expect($resumed->next()['id'])->toBe($missedNew);
    expect($resumed->next()['id'])->toBe($missedOld);
    expect($resumed->attached()['id'])->toBe($missedOld);
    $live = $publish('new', 'live');
    expect($resumed->next()['id'])->toBe($live);
})->with(['memory', 'redis']);

it('repeats a below-floor resume gap on an empty memory log until it can supply a numbered marker', function () {
    $log = new MemoryEventLog(ids: new EventIdGenerator(fn (): int => 10));
    $h = ServerHarness::start(log: $log);
    $path = $h->url('/sse?topics=news');

    foreach ([null, null, '10-1'] as $tail) {
        if ($tail !== null) {
            $log->append(new Event('other', 'first-in-new-log'));
        }

        $stale = SseReader::open($path, ['Last-Event-ID' => '9-0']);
        $stale->start(replaying: true);
        expect($stale->next()['event'])->toBe('murmur.gap');
        expect($stale->attached()['id'])->toBe($tail);
        $stale->close();
    }

    $resumed = SseReader::open($path, ['Last-Event-ID' => '10-1']);
    $resumed->start(replaying: true);
    expect($resumed->attached()['id'])->toBe('10-1');
});

it('resumes a quiet subscription without a gap after unrelated traffic trims its previous cursor', function (string $driver) {
    [$log, $publish] = attachTestLog($driver, capacity: 2);
    $h = ServerHarness::start(['heartbeatSeconds' => 0.2], $log);
    $previous = $publish('old', 'before-attach');
    $initial = SseReader::open($h->url('/sse?topics=old'));
    $initial->start(replaying: true);
    expect($initial->attached()['id'])->toBe($previous);

    $dispatched = new Deferred;
    $log->subscribe(function (Event $event) use ($dispatched): void {
        if ($event->name === 'last-noise') {
            $dispatched->resolve(null);
        }
    });
    for ($i = 0; $i < 6; $i++) {
        $publish('other', 'noise');
    }
    $latest = $publish('other', 'last-noise');
    await(timeout($dispatched->promise(), 1.0));

    $deadline = hrtime(true) + 1_000_000_000;
    do {
        $heartbeat = $initial->next();
    } while ($heartbeat['id'] !== $latest && hrtime(true) < $deadline);
    expect($heartbeat)->toMatchArray(['event' => 'murmur.heartbeat', 'id' => $latest, 'data' => '{}']);
    $initial->close();

    $stale = SseReader::open($h->url('/sse?topics=old'), ['Last-Event-ID' => $previous]);
    $stale->start(replaying: true);
    expect($stale->next()['event'])->toBe('murmur.gap');
    expect($stale->attached()['id'])->toBe($latest);
    $stale->close();

    $resumed = SseReader::open($h->url('/sse?topics=old'), ['Last-Event-ID' => $heartbeat['id']]);
    $resumed->start(replaying: true);
    expect($resumed->attached()['id'])->toBe($latest);
})->with(['memory', 'redis']);

it('resets a client cursor ahead of the log to the tail without signalling a gap', function (string $driver, bool $fresh) {
    [$log, $publish] = attachTestLog($driver);
    $h = ServerHarness::start(log: $log);
    $tail = $publish('news', 'tail');
    $path = '/sse?topics=news&lastEventId=999999999999999999-0'.($fresh ? '&fresh=news' : '');
    $r = SseReader::open($h->url($path));
    $r->start(replaying: true);
    expect($r->nextRaw())->toBe("id: {$tail}\nevent: murmur.attached\ndata: {}");

    $live = $publish('news', 'live');
    expect($r->next()['id'])->toBe($live);
})->with(['memory', 'redis'], [false, true]);

it('supplies a new cursor in a heartbeat after an empty log could not number the marker', function (string $driver) {
    [$log, $publish] = attachTestLog($driver);
    $h = ServerHarness::start(['heartbeatSeconds' => 0.1], $log);
    $path = $h->url('/sse?topics=news');
    $cursor = '999999999999999999-0';

    for ($i = 0; $i < 2; $i++) {
        $r = SseReader::open($path, ['Last-Event-ID' => $cursor]);
        $r->start(replaying: true);
        expect($r->attached()['id'])->toBeNull();
        expect($r->nextRaw())->toBe(': ping');

        if ($i === 0) {
            $r->close();
        }
    }

    $id = $publish('unsubscribed', 'first-in-new-log');
    $deadline = hrtime(true) + 1_000_000_000;

    do {
        $frame = $r->next();
    } while ($frame['comment'] === 'ping' && hrtime(true) < $deadline);

    expect($frame)->toMatchArray(['event' => 'murmur.heartbeat', 'id' => $id, 'data' => '{}']);
    $r->close();

    $resumed = SseReader::open($path, ['Last-Event-ID' => $frame['id']]);
    $resumed->start(replaying: true);
    expect($resumed->attached()['id'])->toBe($id);
})->with(['memory', 'redis']);
