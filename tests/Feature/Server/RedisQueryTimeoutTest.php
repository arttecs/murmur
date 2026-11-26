<?php

use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Log\RedisEventLog;
use Clue\Redis\Protocol\Factory as ProtocolFactory;
use React\Promise\Deferred;
use React\Promise\Timer\TimeoutException;
use React\Socket\ConnectionInterface;
use React\Socket\SocketServer;

use function React\Async\await;
use function React\Promise\Timer\timeout;

it('times out unanswered replay queries and opens a fresh redis connection', function (string $stalledCommand) {
    $socket = new SocketServer('127.0.0.1:0');
    $log = new RedisEventLog(str_replace('tcp://', 'redis://', $socket->getAddress()), 'test', queryTimeoutSeconds: 0.05);
    $clients = [];
    $firstClosed = new Deferred;

    $socket->on('connection', function (ConnectionInterface $client) use (&$clients, $firstClosed, $stalledCommand): void {
        $clients[] = $client;
        $first = count($clients) === 1;
        $parser = (new ProtocolFactory)->createRequestParser();

        if ($first) {
            $client->once('close', fn () => $firstClosed->resolve(null));
        }

        $client->on('data', function (string $data) use ($parser, $client, $first, $stalledCommand): void {
            foreach ($parser->pushIncoming($data) as $message) {
                $command = strtoupper($message->getValueNative()[0]);

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
        expect(fn () => await(timeout($log->since($cursor, ['news'], 10), 1.0)))
            ->toThrow(TimeoutException::class, 'Timed out after 0.05 seconds');
        await(timeout($firstClosed->promise(), 1.0));

        $replay = await(timeout($log->since($cursor, ['news'], 10), 1.0));

        expect($clients)->toHaveCount(2);
        expect($replay->events)->toBe([]);
        expect($replay->gap)->toBeFalse();
    } finally {
        $log->close();
        $socket->close();

        foreach ($clients as $client) {
            $client->close();
        }
    }
})->with(['XREVRANGE', 'XRANGE', 'XINFO']);

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

it('rejects invalid redis reader timing settings', function (string $setting, int|float $value) {
    expect(fn () => new RedisEventLog('redis://127.0.0.1:6379', 'test', ...[$setting => $value]))
        ->toThrow(InvalidArgumentException::class, $setting);
})->with([
    ['queryTimeoutSeconds', 0.0],
    ['queryTimeoutSeconds', NAN],
    ['blockMs', 0],
]);
