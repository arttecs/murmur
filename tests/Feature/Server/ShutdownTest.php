<?php

use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Server\Server;
use Arttecs\Murmur\Server\ServerConfig;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SseReader;
use React\Promise\Deferred;
use React\Socket\Connector;

use function React\Async\await;
use function React\Promise\Timer\timeout;

it('ends streams in order with a retry frame and keeps delivering to the streams still open', function () {
    $config = new ServerConfig('test-secret-with-at-least-32-bytes', port: ServerHarness::availablePort(), retryMs: 200, shutdownSpreadSeconds: 0.4);
    $log = new MemoryEventLog;
    $server = new class($config, $log) extends Server
    {
        protected function randomRetryMs(int $min, int $max): int
        {
            return intdiv($min + $max, 2);
        }
    };

    try {
        await($server->listen());
        $url = $server->address().'/sse?topics=news';
        $first = SseReader::open($url);
        $second = SseReader::open($url);
        expect($first->next()['retry'])->toBe('200');
        expect($second->next()['retry'])->toBe('200');

        $shutdown = $server->shutdown();
        expect($first->next()['retry'])->toBe('300');
        expect(fn () => $first->next())->toThrow(RuntimeException::class, 'stream closed');
        expect($server->metrics()->value('murmur_connections'))->toBe(1);
        $log->append(new Event('news', 'while-draining'));
        expect($second->next()['event'])->toBe('while-draining');
        expect($second->next()['retry'])->toBe('300');
        expect(fn () => $second->next())->toThrow(RuntimeException::class, 'stream closed');
        await(timeout($shutdown, 1.0));
    } finally {
        $server->close();
    }
});

it('answers accepted ready and subscription requests with 503 while draining and refuses new sockets', function () {
    $h = ServerHarness::start(['shutdownSpreadSeconds' => 0.5]);
    $address = substr($h->url(''), strlen('http://'));
    $ready = $h->connect();
    $sse = $h->connect();
    $reader = SseReader::open($h->url('/sse?topics=news'));
    $reader->next();

    $responses = [];
    foreach (['/ready' => $ready, '/sse?topics=news' => $sse] as $path => $client) {
        $response = new Deferred;
        $responses[$path] = $response->promise();
        $buffer = new stdClass;
        $buffer->data = '';
        $client->on('data', fn (string $chunk) => $buffer->data .= $chunk);
        $client->once('close', fn () => $response->resolve($buffer->data));
    }

    try {
        $shutdown = $h->shutdown();
        expect(fn () => await(timeout((new Connector)->connect($address), 0.5)))->toThrow(RuntimeException::class);
        $ready->write("GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n");
        $sse->write("GET /sse?topics=news HTTP/1.1\r\nHost: localhost\r\n\r\n");

        expect(await(timeout($responses['/ready'], 1.0)))->toContain('503 Service Unavailable', 'Cache-Control: no-store', '{"status":"unavailable"}');
        expect(await(timeout($responses['/sse?topics=news'], 1.0)))->toContain('503 Service Unavailable');
        await(timeout($shutdown, 1.0));
    } finally {
        $ready->close();
        $sse->close();
    }
});

it('ends every open stream immediately when spreading is disabled', function () {
    $h = ServerHarness::start(['shutdownSpreadSeconds' => 0.0]);
    $first = SseReader::open($h->url('/sse?topics=news'));
    $second = SseReader::open($h->url('/sse?topics=news'));
    $first->next();
    $second->next();

    $shutdown = $h->shutdown();

    foreach ([$first, $second] as $reader) {
        expect((int) $reader->next(0.5)['retry'])->toBeGreaterThanOrEqual(3000)->toBeLessThanOrEqual(6000);
        expect(fn () => $reader->next())->toThrow(RuntimeException::class, 'stream closed');
    }

    await(timeout($shutdown, 0.5));
    expect($h->trackedSocketCount())->toBe(0);
});
