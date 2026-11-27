<?php

use Arttecs\Murmur\Server\Auth\Grant;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\Http\SseHandler;
use Arttecs\Murmur\Server\Hub;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Server\Log\RedisEventLog;
use Arttecs\Murmur\Server\PeriodicTimer;
use Arttecs\Murmur\Server\Server;
use Arttecs\Murmur\Server\ServerConfig;
use Arttecs\Murmur\Tests\Support\CallbackSubscriptionAuthorizer;
use Arttecs\Murmur\Tests\Support\ReadyTimerLoop;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SseReader;
use Clue\React\Redis\Client;
use React\EventLoop\ExtUvLoop;
use React\EventLoop\Loop;
use React\Http\Message\ServerRequest;
use React\Promise\Deferred;
use React\Socket\ConnectionInterface;

use function React\Async\await;
use function React\Promise\resolve;

it('preserves the backend cadence for sub-millisecond repeating timers', function () {
    $backend = Loop::get()::class;
    $loop = new $backend;
    $interval = 0.0005;
    $timer = null;
    $next = null;
    $pending = new ReflectionProperty(PeriodicTimer::class, 'timer');

    $timer = new PeriodicTimer($loop, $interval, function () use (&$timer, &$next, $pending): void {
        $next = $pending->getValue($timer);
        $timer->cancel();
    });

    expect($pending->getValue($timer)->getInterval())->toBe($interval);
    $loop->run();

    expect($next->getInterval())->toBe($loop instanceof ExtUvLoop ? 0.001 : $interval);
});

it('does not repeat ready server timers after close', function (int $timerIndex) {
    $loop = new ReadyTimerLoop(Loop::get());
    $server = new Server(new ServerConfig(str_repeat('x', 32), port: ServerHarness::availablePort()), new MemoryEventLog, $loop);

    try {
        await($server->listen());
        $loop->queue($loop->timers()[$timerIndex]);
        $server->close();
        $loop->dispatch();

        expect($loop->timers())->toBeEmpty();
    } finally {
        $server->close();
    }
})->with(['heartbeat' => [0], 'loop lag' => [1]]);

it('does not rearm a ready expiry timer after an SSE response closes', function () {
    $loop = new ReadyTimerLoop(Loop::get());
    $handler = new SseHandler(
        new ServerConfig(str_repeat('x', 32)),
        new MemoryEventLog,
        new Hub,
        $loop,
        new CallbackSubscriptionAuthorizer(fn () => resolve(new Grant(['private-a'], time() + 600))),
        fn () => true,
    );
    $client = $this->createStub(ConnectionInterface::class);
    $client->method('isWritable')->willReturn(true);
    $request = (new ServerRequest('GET', '/sse'))->withQueryParams(['topics' => 'private-a', 'token' => 'opaque']);
    $response = await($handler($request, $client));

    try {
        expect($response->getStatusCode())->toBe(200);
        $loop->queue($loop->timers()[0]);
    } finally {
        $response->getBody()->close();
    }

    $loop->dispatch();

    expect($loop->timers())->toBeEmpty();
});

it('keeps serving SSE when a ready request deadline was cancelled by the response', function () {
    $loop = new ReadyTimerLoop(Loop::get());
    $log = new MemoryEventLog;
    $authorizer = new CallbackSubscriptionAuthorizer(function () use ($loop) {
        $loop->queue($loop->timers()[2]);

        return resolve(new Grant(['news'], time() + 600));
    });
    $config = new ServerConfig(str_repeat('x', 32), port: ServerHarness::availablePort());
    $server = new Server($config, $log, $loop, authorizer: $authorizer);

    try {
        await($server->listen());
        $reader = SseReader::open($server->address().'/sse?topics=news');
        $reader->start();
        $loop->dispatch();

        expect($server->metrics()->value('murmur_connections'))->toBe(1);
        $log->append(new Event('news', 'still-open'));
        expect($reader->next()['event'])->toBe('still-open');
    } finally {
        $server->close();
    }
});

it('keeps the Redis reader when a ready deadline belongs to a completed read', function () {
    $loop = new ReadyTimerLoop(Loop::get());
    $log = new RedisEventLog('redis://127.0.0.1:6379', 'test', loop: $loop);
    $first = new Deferred;
    $second = new Deferred;
    $reader = $this->createMock(Client::class);
    $reader->expects($this->exactly(3))->method('__call')
        ->willReturnOnConsecutiveCalls(resolve([]), $first->promise(), $second->promise());
    $property = new ReflectionProperty(RedisEventLog::class, 'reader');
    $property->setValue($log, $reader);

    try {
        $log->subscribe(fn () => null);
        $loop->queue($loop->timers()[0]);
        $first->resolve(null);
        $nextDeadline = $loop->timers()[0];
        $loop->dispatch();

        expect($property->getValue($log))->toBe($reader)
            ->and($loop->timers())->toBe([$nextDeadline]);
    } finally {
        $log->close();
    }
});

it('does not query Redis when a ready reader retry was cancelled by close', function () {
    $loop = new ReadyTimerLoop(Loop::get());
    $log = new RedisEventLog('redis://127.0.0.1:6379', 'test', loop: $loop);
    $read = new Deferred;
    $reader = $this->createMock(Client::class);
    $reader->expects($this->exactly(2))->method('__call')
        ->willReturnOnConsecutiveCalls(resolve([]), $read->promise());
    (new ReflectionProperty(RedisEventLog::class, 'reader'))->setValue($log, $reader);

    try {
        $log->subscribe(fn () => null);
        (new ReflectionProperty(RedisEventLog::class, 'readerDisconnected'))->setValue($log, true);
        $read->reject(new RuntimeException('Reader disconnected.'));
        $loop->queue($loop->timers()[0]);
    } finally {
        $log->close();
    }

    $loop->dispatch();

    expect($loop->timers())->toBeEmpty();
});
