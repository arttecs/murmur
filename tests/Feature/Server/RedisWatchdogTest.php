<?php

use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\Log\RedisEventLog;
use Clue\Redis\Protocol\Factory as ProtocolFactory;
use React\Promise\Deferred;
use React\Socket\ConnectionInterface;
use React\Socket\SocketServer;

use function React\Async\await;
use function React\Promise\Timer\timeout;

it('replaces a stalled redis reader and reports only the loss and recovery transitions', function () {
    $errorLog = tempnam(sys_get_temp_dir(), 'murmur');
    $previous = ini_set('error_log', $errorLog);
    $socket = new SocketServer('127.0.0.1:0');
    $log = new RedisEventLog(str_replace('tcp://', 'redis://', $socket->getAddress()), 'test');
    $delivered = new Deferred;
    $clients = [];
    $reads = [];
    $firstClosed = false;

    $socket->on('connection', function (ConnectionInterface $client) use (&$clients, &$reads, &$firstClosed): void {
        $clients[] = $client;
        $attempt = count($clients);
        $parser = (new ProtocolFactory)->createRequestParser();

        if ($attempt === 1) {
            $client->once('close', function () use (&$firstClosed): void {
                $firstClosed = true;
            });
        }

        $client->on('data', function (string $data) use ($parser, $client, $attempt, &$reads): void {
            foreach ($parser->pushIncoming($data) as $message) {
                $command = $message->getValueNative();

                if (strtoupper($command[0]) === 'XREVRANGE') {
                    $client->write("*1\r\n*2\r\n\$3\r\n1-0\r\n*0\r\n");

                    continue;
                }

                if (strtoupper($command[0]) !== 'XREAD') {
                    continue;
                }

                $reads[] = $command;

                if ($attempt === 2) {
                    $client->close();
                } elseif ($attempt === 3 && count($reads) === 3) {
                    $client->write("*1\r\n*2\r\n\$4\r\ntest\r\n*1\r\n*2\r\n\$3\r\n2-0\r\n*6\r\n\$5\r\ntopic\r\n\$4\r\nnews\r\n\$4\r\nname\r\n\$9\r\nrecovered\r\n\$4\r\ndata\r\n\$4\r\nnull\r\n");
                }
            }
        });
    });

    try {
        $started = hrtime(true);
        $log->subscribe(fn (Event $event) => $delivered->resolve($event));
        $event = await(timeout($delivered->promise(), 14.0));

        expect($firstClosed)->toBeTrue();
        expect(hrtime(true) - $started)->toBeGreaterThanOrEqual(11_900_000_000);
        expect(array_map(fn (array $read) => array_slice($read, 1), array_slice($reads, 0, 3)))
            ->toBe(array_fill(0, 3, ['BLOCK', '5000', 'STREAMS', 'test', '1-0']));
        expect((string) $event->id)->toBe('2-0');

        $lines = file($errorLog, FILE_IGNORE_NEW_LINES);
        expect($lines)->toHaveCount(2);
        expect($lines[0])->toContain('Murmur Redis reader lost its connection.');
        expect($lines[1])->toContain('Murmur Redis reader recovered.');
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
