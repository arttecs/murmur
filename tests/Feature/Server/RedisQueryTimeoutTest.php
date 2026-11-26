<?php

use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Log\RedisEventLog;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SseReader;
use Clue\Redis\Protocol\Factory as ProtocolFactory;
use React\Promise\Deferred;
use React\Promise\Timer\TimeoutException;
use React\Socket\ConnectionInterface;
use React\Socket\SocketServer;

use function React\Async\await;
use function React\Async\delay;
use function React\Promise\Timer\timeout;

it('times out unanswered replay queries and opens a fresh redis connection', function (string $stalledCommand, array $topics) {
    $socket = new SocketServer('127.0.0.1:0');
    $log = new RedisEventLog(str_replace('tcp://', 'redis://', $socket->getAddress()), 'test', queryTimeoutSeconds: 0.05);
    $clients = [];
    $commands = [];
    $firstClosed = new Deferred;

    $socket->on('connection', function (ConnectionInterface $client) use (&$clients, &$commands, $firstClosed, $stalledCommand): void {
        $clients[] = $client;
        $first = count($clients) === 1;
        $parser = (new ProtocolFactory)->createRequestParser();

        if ($first) {
            $client->once('close', fn () => $firstClosed->resolve(null));
        }

        $client->on('data', function (string $data) use ($parser, $client, $first, &$commands, $stalledCommand): void {
            foreach ($parser->pushIncoming($data) as $message) {
                $arguments = $message->getValueNative();
                $command = strtoupper($arguments[0]);

                $commands[] = $command;

                if ($command === 'XRANGE') {
                    expect($arguments[3])->toBe('1-0');
                }

                if ($first && $command === $stalledCommand) {
                    continue;
                }

                $client->write(match ($command) {
                    'XREVRANGE' => "*1\r\n*2\r\n$3\r\n1-0\r\n*0\r\n",
                    'XINFO' => "-ERR no such key\r\n",
                    default => "*0\r\n",
                });
            }
        });
    });

    try {
        $cursor = new EventId(0, 0);
        expect(fn () => await(timeout($log->since($cursor, $topics, 10), 1.0)))
            ->toThrow(TimeoutException::class, 'Timed out after 0.05 seconds');
        await(timeout($firstClosed->promise(), 1.0));

        $commands = [];
        $replay = await(timeout($log->since($cursor, $topics, 10), 1.0));

        expect($clients)->toHaveCount(2);
        expect($replay->events)->toBe([]);
        expect($replay->gap)->toBeFalse();
        expect((string) $replay->tail)->toBe('1-0');
        expect($commands)->toBe($topics === [] ? ['XREVRANGE'] : ['XREVRANGE', 'XRANGE', 'XINFO']);
    } finally {
        $log->close();
        $socket->close();

        foreach ($clients as $client) {
            $client->close();
        }
    }
})->with([
    'tail' => ['XREVRANGE', ['news']],
    'range' => ['XRANGE', ['news']],
    'stream info' => ['XINFO', ['news']],
    'tail only' => ['XREVRANGE', []],
]);

it('reports each subscription that loses its tail sample or replay on the shared redis connection', function (string $stalledCommand) {
    delay(1.01);
    $errorLog = tempnam(sys_get_temp_dir(), 'murmur');
    $previous = ini_set('error_log', $errorLog);
    $socket = new SocketServer('127.0.0.1:0');
    $log = new RedisEventLog(str_replace('tcp://', 'redis://', $socket->getAddress()), 'test', queryTimeoutSeconds: 0.25);
    $clients = [];
    $queued = new Deferred;

    $socket->on('connection', function (ConnectionInterface $client) use (&$clients, $queued, $stalledCommand): void {
        $clients[] = $client;
        $shared = count($clients) === 2;
        $parser = (new ProtocolFactory)->createRequestParser();
        $pending = 0;

        $client->on('data', function (string $data) use ($parser, $client, $shared, $queued, $stalledCommand, &$pending): void {
            foreach ($parser->pushIncoming($data) as $message) {
                $command = strtoupper($message->getValueNative()[0]);

                if ($shared && ($pending > 0 || $command === $stalledCommand)) {
                    if (++$pending === 2) {
                        $queued->resolve(null);
                    }

                    continue;
                }

                if ($command === 'XREVRANGE') {
                    $client->write("*1\r\n*2\r\n\$3\r\n1-0\r\n*0\r\n");
                }
            }
        });
    });

    try {
        $h = ServerHarness::start(log: $log);
        $path = $h->url('/sse?topics=news');
        $first = SseReader::open($path, ['Last-Event-ID' => '0-0']);
        $first->start(replaying: true);
        $second = SseReader::open($path);
        $second->start(replaying: true);
        await(timeout($queued->promise(), 1.0));

        foreach ([$first, $second] as $reader) {
            expect(fn () => $reader->nextRaw())->toThrow(RuntimeException::class, 'SSE stream closed');
        }

        expect($h->metrics()->value('murmur_replay_failures_total'))->toBe(2);
        expect($h->metrics()->value('murmur_errors_total'))->toBe(2);
        expect(file($errorLog, FILE_IGNORE_NEW_LINES))->toHaveCount(1);

        $recovered = SseReader::open($path);
        $recovered->start(replaying: true);
        expect($recovered->attached()['id'])->toBe('1-0');
        expect($h->metrics()->value('murmur_replay_failures_total'))->toBe(2);
    } finally {
        $log->close();
        $socket->close();

        foreach ($clients as $client) {
            $client->close();
        }

        ini_set('error_log', (string) $previous);
        unlink($errorLog);
    }
})->with(['XREVRANGE', 'XRANGE']);

it('retries an unanswered initial cursor query on a fresh redis connection', function () {
    $errorLog = tempnam(sys_get_temp_dir(), 'murmur');
    $previous = ini_set('error_log', $errorLog);
    $socket = new SocketServer('127.0.0.1:0');
    $log = new RedisEventLog(str_replace('tcp://', 'redis://', $socket->getAddress()), 'test', queryTimeoutSeconds: 0.05);
    $clients = [];

    $socket->on('connection', function (ConnectionInterface $client) use (&$clients): void {
        $clients[] = $client;

        if (count($clients) === 1) {
            return;
        }

        $parser = (new ProtocolFactory)->createRequestParser();
        $client->on('data', function (string $data) use ($client, $parser): void {
            foreach ($parser->pushIncoming($data) as $message) {
                if (strtoupper($message->getValueNative()[0]) === 'XREVRANGE') {
                    $client->write("*0\r\n");
                }
            }
        });
    });

    try {
        $log->subscribe(fn () => null);
        await(timeout($log->ready(), 2.0));

        expect($clients)->toHaveCount(2);
        expect(file($errorLog, FILE_IGNORE_NEW_LINES))->toHaveCount(2);
        expect(file_get_contents($errorLog))->toContain('Murmur Redis reader recovered.');
    } finally {
        $log->close();
        $socket->close();

        foreach ($clients as $client) {
            $client->close();
        }

        ini_set('error_log', (string) $previous);
        unlink($errorLog);
    }
});

it('retries from the previous cursor if a live trim check fails', function (bool $stalled) {
    $errorLog = tempnam(sys_get_temp_dir(), 'murmur');
    $previous = ini_set('error_log', $errorLog);
    $socket = new SocketServer('127.0.0.1:0');
    $log = new RedisEventLog(str_replace('tcp://', 'redis://', $socket->getAddress()), 'test', maxLength: 1, queryTimeoutSeconds: 0.05);
    $clients = [];
    $reads = [];
    $checks = 0;
    $seen = [];
    $delivered = new Deferred;
    $factory = new ProtocolFactory;
    $serializer = $factory->createSerializer();
    $entry = fn (string $id, string $name): array => [$id, ['topic', 'news', 'name', $name, 'data', 'null']];

    $socket->on('connection', function (ConnectionInterface $client) use (&$clients, &$reads, &$checks, $factory, $serializer, $entry, $stalled): void {
        $clients[] = $client;
        $parser = $factory->createRequestParser();

        $client->on('data', function (string $data) use ($parser, $client, &$reads, &$checks, $serializer, $entry, $stalled): void {
            foreach ($parser->pushIncoming($data) as $message) {
                $arguments = $message->getValueNative();

                switch (strtoupper($arguments[0])) {
                    case 'XREVRANGE':
                        $client->write($serializer->getReplyMessage([$entry('1-0', 'before')]));
                        break;
                    case 'XREAD':
                        $reads[] = $arguments[5];

                        if (count($reads) === 1) {
                            $client->write($serializer->getReplyMessage([['test', [$entry('2-0', 'unverified'), $entry('3-0', 'unverified')]]]));
                        } elseif (count($reads) === 2) {
                            $client->write($serializer->getReplyMessage([['test', [$entry('4-0', 'retained')]]]));
                        }
                        break;
                    case 'XINFO':
                        if (++$checks === 1) {
                            if (! $stalled) {
                                $client->write("-ERR trim check failed\r\n");
                            }

                            break;
                        }

                        $client->write($serializer->getReplyMessage(['length', 1, 'entries-added', 4, 'first-entry', $entry('4-0', 'retained')]));
                        break;
                }
            }
        });
    });

    try {
        $log->onGap(function () use (&$seen): void {
            $seen[] = 'gap';
        });
        $log->subscribe(function (Event $event) use (&$seen, $delivered): void {
            $seen[] = $event->name;

            if ($event->name === 'retained') {
                $delivered->resolve(null);
            }
        });

        await(timeout($delivered->promise(), 3.0));

        expect($seen)->toBe(['gap', 'retained']);
        expect(array_slice($reads, 0, 2))->toBe(['1-0', '1-0']);
        expect($clients)->toHaveCount($stalled ? 2 : 1);
    } finally {
        $log->close();
        $socket->close();

        foreach ($clients as $client) {
            $client->close();
        }

        ini_set('error_log', (string) $previous);
        unlink($errorLog);
    }
})->with(['error reply' => [false], 'deadline' => [true]]);

it('rejects invalid redis reader timing settings', function (string $setting, int|float $value) {
    expect(fn () => new RedisEventLog('redis://127.0.0.1:6379', 'test', ...[$setting => $value]))
        ->toThrow(InvalidArgumentException::class, $setting);
})->with([
    ['queryTimeoutSeconds', 0.0],
    ['queryTimeoutSeconds', NAN],
    ['blockMs', 0],
]);
