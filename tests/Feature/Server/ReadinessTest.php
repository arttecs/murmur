<?php

use Arttecs\Murmur\Server\Log\RedisEventLog;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Clue\Redis\Protocol\Factory as ProtocolFactory;
use React\Http\Browser;
use React\Promise\Deferred;
use React\Socket\ConnectionInterface;
use React\Socket\SocketServer;

use function React\Async\await;
use function React\Async\delay;
use function React\Promise\Timer\timeout;

it('reports readiness independently from liveness and disables caching', function () {
    $h = ServerHarness::start();
    $browser = (new Browser)->withRejectErrorResponse(false);
    $ready = await($browser->get($h->url('/ready')));

    expect($ready->getStatusCode())->toBe(200);
    expect((string) $ready->getBody())->toBe('{"status":"ready"}');
    expect($ready->getHeaderLine('Cache-Control'))->toBe('no-store');

    $h->log->close();
    expect($h->log->healthy())->toBeFalse();
    $unavailable = await($browser->get($h->url('/ready')));
    expect($unavailable->getStatusCode())->toBe(503);
    expect((string) $unavailable->getBody())->toBe('{"status":"unavailable"}');
    expect($unavailable->getHeaderLine('Cache-Control'))->toBe('no-store');
    expect((string) $h->pollHealth(0)->getBody())->toBe('{"status":"ok","connections":0}');
});

it('reports not ready when the Redis reader disconnects and ready after recovery', function () {
    $errorFile = tempnam(sys_get_temp_dir(), 'murmur');
    $previousErrorLog = ini_set('error_log', $errorFile);
    $socket = new SocketServer('127.0.0.1:0');
    $url = str_replace('tcp://', 'redis://', $socket->getAddress());
    $log = new RedisEventLog($url, 'readiness', blockMs: 100);
    $clients = [];
    $recovering = new Deferred;
    $recovered = new Deferred;
    $socket->on('connection', function (ConnectionInterface $client) use (&$clients, $recovering, $recovered): void {
        $clients[] = $client;
        $attempt = count($clients);
        $parser = (new ProtocolFactory)->createRequestParser();
        $reads = 0;

        $client->on('data', function (string $data) use ($client, $attempt, $parser, $recovering, $recovered, &$reads): void {
            foreach ($parser->pushIncoming($data) as $message) {
                switch (strtoupper($message->getValueNative()[0])) {
                    case 'XREVRANGE':
                        $client->write("*0\r\n");
                        break;
                    case 'XINFO':
                        $client->write("-ERR no such key\r\n");
                        break;
                    case 'XREAD':
                        if ($attempt > 1) {
                            match (++$reads) {
                                1 => $recovering->resolve($client),
                                2 => $recovered->resolve(null),
                                default => null,
                            };
                        }
                        break;
                }
            }
        });
    });

    try {
        expect($log->healthy())->toBeFalse();
        $h = ServerHarness::start(log: $log);
        $browser = (new Browser)->withRejectErrorResponse(false);
        expect(await($browser->get($h->url('/ready')))->getStatusCode())->toBe(200);

        $clients[0]->close();
        delay(0.01);
        expect(await($browser->get($h->url('/ready')))->getStatusCode())->toBe(503);
        expect($h->pollHealth(0)->getStatusCode())->toBe(200);

        $reader = await(timeout($recovering->promise(), 2.0));
        expect(await($browser->get($h->url('/ready')))->getStatusCode())->toBe(503);
        $reader->write("*-1\r\n");
        await(timeout($recovered->promise(), 1.0));
        expect(await($browser->get($h->url('/ready')))->getStatusCode())->toBe(200);
    } finally {
        $log->close();
        $socket->close();

        foreach ($clients as $client) {
            $client->close();
        }

        ini_set('error_log', (string) $previousErrorLog);
        unlink($errorFile);
    }
});
