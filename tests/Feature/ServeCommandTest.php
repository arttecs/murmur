<?php

use Arttecs\Murmur\Console\Commands\ServeCommand;
use Arttecs\Murmur\Contracts\SubscriptionAuthorizer;
use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\Server\Auth\Grant;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Server\Log\RedisEventLog;
use Arttecs\Murmur\Server\Server;
use Arttecs\Murmur\Tests\Support\CallbackSubscriptionAuthorizer;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SseReader;
use Clue\Redis\Protocol\Factory as ProtocolFactory;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Support\ServiceProvider;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\EventLoop\StreamSelectLoop;
use React\Promise\Deferred;
use React\Promise\PromiseInterface;
use React\Promise\Timer\TimeoutException;
use React\Socket\ConnectionInterface;
use React\Socket\SocketServer;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Process\Process;

use function React\Async\async;
use function React\Async\await;
use function React\Async\delay;
use function React\Promise\resolve;
use function React\Promise\Timer\timeout;

it('maps laravel config and options onto the server config', function () {
    config([
        'murmur.server.heartbeat' => 5,
        'murmur.server.backlog' => 2048,
        'murmur.server.allowed_origins' => ['https://app.test'],
        'murmur.server.max_sockets' => 950,
        'murmur.server.request_timeout' => 3.5,
        'murmur.server.max_replay_events' => 15,
        'murmur.server.signature_tolerance' => 30,
        'murmur.server.shutdown_grace' => 0.5,
        'murmur.server.shutdown_spread' => 0.2,
        'murmur.server.loop_lag_threshold_ms' => 25,
    ]);
    $command = app(ServeCommand::class);
    $command->setLaravel(app());
    $command->setInput(new ArrayInput(['--port' => '9999'], $command->getDefinition()));
    $config = $command->makeConfig();
    expect($config->port)->toBe(9999);
    expect($config->backlog)->toBe(2048);
    expect($config->host)->toBe('127.0.0.1');
    expect($config->heartbeatSeconds)->toBe(5.0);
    expect($config->maxConnections)->toBe(900);
    expect($config->maxSockets)->toBe(950);
    expect($config->requestTimeoutSeconds)->toBe(3.5);
    expect($config->maxReplayEvents)->toBe(15);
    expect($config->signatureToleranceSeconds)->toBe(30);
    expect($config->loopLagThresholdMs)->toBe(25);
    expect($config->shutdownGraceSeconds)->toBe(0.5);
    expect($config->shutdownSpreadSeconds)->toBe(0.2);
    expect($config->allowedOrigins)->toBe(['https://app.test']);
    expect($config->secret)->toBe('test-secret-with-at-least-32-bytes');
});

it('refuses to start above the stream select socket limit', function () {
    $previousLoop = Loop::get();
    $loop = new StreamSelectLoop;
    Loop::set($loop);
    $loop->addTimer(0.0, $loop->stop(...));
    config(['murmur.server.max_sockets' => 1001]);

    try {
        $this->artisan('murmur:serve')
            ->expectsOutputToContain('stream_select')
            ->assertExitCode(1);
    } finally {
        Loop::set($previousLoop);
    }
});

it('refuses to start without a secret', function () {
    config(['murmur.secret' => null]);
    $this->artisan('murmur:serve')->assertExitCode(1);
});

it('explains that a short secret is not accepted', function () {
    config(['murmur.secret' => 'short']);
    $this->artisan('murmur:serve')
        ->expectsOutputToContain('at least 32 bytes')
        ->assertExitCode(1);
});

it('builds an in-memory log sized by the config for the http driver', function () {
    config(['murmur.server.log_size' => 1]);
    $log = app(ServeCommand::class)->makeLog();

    expect($log)->toBeInstanceOf(MemoryEventLog::class);

    $first = $log->append(new Event('news', 'a'));
    $log->append(new Event('news', 'b'));
    $log->append(new Event('news', 'c'));

    expect(await($log->since($first->id, ['news'], 1000))->gap)->toBeTrue();
});

it('rejects an unknown log driver', function () {
    config(['murmur.driver' => 'carrier-pigeon']);
    app(ServeCommand::class)->makeLog();
})->throws(InvalidArgumentException::class);

it('uses the configured redis stream and log size for the redis driver', function () {
    [, $key, $url] = redisStream();
    $prefix = 'murmur:';
    config([
        'murmur.driver' => 'redis',
        'murmur.redis.url' => $url,
        'murmur.redis.stream' => substr($key, strlen($prefix)),
        'murmur.server.log_size' => 1,
        'database.redis.options.prefix' => $prefix,
    ]);
    $first = xadd($key, 'news', 'first');
    $id = xadd($key, 'news', 'updated', maxLength: 1);
    $log = app(ServeCommand::class)->makeLog();

    try {
        expect($log)->toBeInstanceOf(RedisEventLog::class);
        $replay = await($log->since(EventId::tryParse($first), ['news'], 1000));
        expect($replay->gap)->toBeTrue();
        expect(array_map(fn (Event $event) => (string) $event->id, $replay->events))->toBe([$id]);
    } finally {
        $log->close();
    }
});

it('disables http publishing for the redis driver', function () {
    config(['murmur.driver' => 'redis']);
    $command = app(ServeCommand::class);
    $command->setLaravel(app());
    $command->setInput(new ArrayInput([], $command->getDefinition()));

    expect($command->makeConfig()->publishEnabled)->toBeFalse();
});

it('reports an unknown driver and exits cleanly', function () {
    config(['murmur.driver' => 'carrier-pigeon']);
    $this->artisan('murmur:serve')
        ->expectsOutputToContain('Unsupported Murmur server driver [carrier-pigeon].')
        ->assertExitCode(1);
});

it('runs murmur serve and exits cleanly on SIGTERM', function (string $driver) {
    $port = ServerHarness::availablePort();
    $address = "http://127.0.0.1:{$port}";
    $waiting = 'Murmur is waiting for Redis.';
    $listening = "Murmur is listening on {$address}";
    $ready = $driver === 'redis' ? $waiting : $listening;
    $root = dirname(__DIR__, 2);
    $process = new Process([PHP_BINARY, '-d', 'error_log=', $root.'/vendor/bin/testbench', 'murmur:serve', "--port={$port}"], $root, [
        'MURMUR_DRIVER' => $driver,
        'MURMUR_HOST' => '127.0.0.1',
        'MURMUR_SECRET' => config('murmur.secret'),
        'MURMUR_REDIS_URL' => 'redis://127.0.0.1:'.ServerHarness::availablePort(),
    ]);
    $output = fn (): string => $process->getOutput().$process->getErrorOutput();
    $reader = null;

    try {
        $process->start();
        $deadline = hrtime(true) + 5_000_000_000;

        while ($process->isRunning() && ! str_contains($output(), $ready) && hrtime(true) < $deadline) {
            delay(0.01);
        }

        expect(str_contains($output(), $ready))->toBeTrue($output());
        expect($process->isRunning())->toBeTrue($output());

        if ($driver === 'redis') {
            delay(1.1);
            expect($process->isRunning())->toBeTrue($output());
            expect(substr_count($output(), $waiting))->toBe(1, $output());
            expect(str_contains($output(), $listening))->toBeFalse($output());
        } else {
            try {
                $reader = await(timeout(async(fn () => SseReader::open($address.'/sse?topics=news'))(), 2.0));
                $frame = $reader->next(2.0);
            } catch (Throwable $error) {
                $this->fail($error->getMessage()."\n".$output());
            }

            expect($frame['retry'])->toBe('3000', $output());
        }

        $process->signal(SIGTERM);
        $deadline = hrtime(true) + 5_000_000_000;

        while ($process->isRunning() && hrtime(true) < $deadline) {
            delay(0.01);
        }

        expect($process->isRunning())->toBeFalse($output());
        expect($process->getExitCode())->toBe(0, $output());
    } finally {
        $process->stop(0.1, SIGKILL);
        $reader?->close();
        assertServingProcessExit($process);
    }
})->with(['http', 'redis'])->skip(! extension_loaded('pcntl'), 'SIGTERM handling requires ext-pcntl.');

it('uses the configured redis query deadline and blocking read duration', function () {
    $socket = new SocketServer('127.0.0.1:0');
    config([
        'murmur.driver' => 'redis',
        'murmur.redis.url' => str_replace('tcp://', 'redis://', $socket->getAddress()),
        'murmur.redis.query_timeout' => 0.05,
        'murmur.redis.block_ms' => 125,
    ]);
    $clients = [];
    $block = new Deferred;

    $socket->on('connection', function (ConnectionInterface $client) use (&$clients, $block): void {
        $clients[] = $client;
        $parser = (new ProtocolFactory)->createRequestParser();
        $client->on('data', function (string $data) use ($client, $parser, $block, &$clients): void {
            foreach ($parser->pushIncoming($data) as $message) {
                $command = $message->getValueNative();

                if (strtoupper($command[0]) === 'XREAD') {
                    $block->resolve($command[array_search('BLOCK', $command, true) + 1]);
                } elseif ($client === $clients[0] && strtoupper($command[0]) === 'XREVRANGE') {
                    $client->write("*0\r\n");
                }
            }
        });
    });

    $log = app(ServeCommand::class)->makeLog();

    try {
        $log->subscribe(fn () => null);
        expect(await(timeout($block->promise(), 1.0)))->toBe('125');
        expect(fn () => await(timeout($log->since(new EventId(0, 0), ['news'], 1), 1.0)))
            ->toThrow(TimeoutException::class, 'Timed out after 0.05 seconds');
    } finally {
        $log->close();
        $socket->close();

        foreach ($clients as $client) {
            $client->close();
        }
    }
});

it('writes server lifecycle messages to the configured laravel log channel', function () {
    $file = tempnam(sys_get_temp_dir(), 'murmur');
    $port = ServerHarness::availablePort();
    config([
        'murmur.server.log_channel' => 'murmur_test',
        'logging.channels.murmur_test' => ['driver' => 'single', 'path' => $file],
    ]);
    $previousLoop = Loop::get();
    $loop = new StreamSelectLoop;
    Loop::set($loop);
    $loop->addTimer(0.0, $loop->stop(...));

    try {
        $this->artisan('murmur:serve', ['--port' => $port])
            ->expectsOutputToContain('ext-uv, ext-ev or ext-event')
            ->expectsOutput("Murmur is listening on http://127.0.0.1:{$port}")
            ->assertSuccessful();

        expect(file_get_contents($file))->toContain("Murmur is listening on http://127.0.0.1:{$port}");
    } finally {
        Loop::set($previousLoop);
        unlink($file);
    }
});

it('hands the server and its loop to serving callbacks before listening', function () {
    $port = ServerHarness::availablePort();
    $previousLoop = Loop::get();
    $loop = new StreamSelectLoop;
    Loop::set($loop);
    $loop->addTimer(0.0, $loop->stop(...));
    $calls = [];
    Murmur::serving(function (Server $server, LoopInterface $given) use (&$calls): void {
        $calls[] = [$server->metrics()->value('murmur_connections_opened_total'), $given];
    });

    try {
        $this->artisan('murmur:serve', ['--port' => $port])->assertSuccessful();

        expect($calls)->toBe([[0, $loop]]);
    } finally {
        Loop::set($previousLoop);
    }
});

it('rejects malformed configured origins at startup', function (mixed $origin) {
    config(['murmur.server.allowed_origins' => [$origin]]);

    $previousLoop = Loop::get();
    $loop = new StreamSelectLoop;
    Loop::set($loop);
    $loop->addTimer(0.0, $loop->stop(...));

    try {
        $this->artisan('murmur:serve', ['--port' => ServerHarness::availablePort()])
            ->expectsOutput('The allowedOrigins setting must contain only * or valid origins without a path, query, or fragment.')
            ->assertFailed();
    } finally {
        Loop::set($previousLoop);
    }
})->with(['https://app.test/', 'https://app.test?x', 'https://app.test#', 'https://user@app.test', 'app.test', 'https://[::invalid]', 'https://app.test:65536', 'https://*.test', "https://app.test\n", 1]);

it('uses the subscription authorizer registered in a laravel provider boot method', function () {
    $provider = new class($this->app) extends ServiceProvider
    {
        public array $resolved = [];

        public function boot(): void
        {
            Murmur::authorizeSubscriptionsUsing(function (Application $app): SubscriptionAuthorizer {
                $this->resolved[] = $app;

                return new CallbackSubscriptionAuthorizer(fn (?string $token): PromiseInterface => resolve(
                    new Grant($token === 'opaque' ? ['private-news'] : [], time() + 30),
                ));
            });
        }
    };
    $this->app->register($provider);
    expect($provider->resolved)->toBe([]);

    $previousLoop = Loop::get();
    $loop = new StreamSelectLoop;
    Loop::set($loop);
    $port = ServerHarness::availablePort();
    $response = new Deferred;
    $loop->futureTick(function () use ($port, $response, $loop): void {
        timeout(async(fn () => SseReader::open("http://127.0.0.1:{$port}/sse?topics=private-news&token=opaque"))(), 1.0)
            ->then($response->resolve(...), $response->reject(...))
            ->finally($loop->stop(...));
    });

    try {
        $this->artisan('murmur:serve', ['--port' => $port])->assertSuccessful()->run();
        expect(await($response->promise())->status())->toBe(200);
        expect($provider->resolved)->toBe([$this->app]);
    } finally {
        Loop::set($previousLoop);
    }
});

it('defers shutdown out of the signal handler so the loop never selects a closed socket', function () {
    $port = ServerHarness::availablePort();
    $previousLoop = Loop::get();
    $loop = new StreamSelectLoop;
    Loop::set($loop);
    $stillListening = null;
    $loop->addTimer(0.05, function () use ($port, &$stillListening): void {
        posix_kill(getmypid(), SIGTERM);
        pcntl_signal_dispatch();
        $client = @stream_socket_client("tcp://127.0.0.1:{$port}", $code, $message, 0.5);
        $stillListening = $client !== false;

        if ($client !== false) {
            fclose($client);
        }
    });

    try {
        $this->artisan('murmur:serve', ['--port' => $port])->assertSuccessful();

        expect($stillListening)->toBeTrue();
    } finally {
        Loop::set($previousLoop);
    }
})->skip(! extension_loaded('pcntl') || ! extension_loaded('posix'), 'Signal handling requires ext-pcntl and ext-posix.');

it('refuses an invalid garbage collection interval', function (mixed $interval) {
    config(['murmur.server.gc_interval' => $interval]);

    $this->artisan('murmur:serve')
        ->expectsOutputToContain('gc_interval')
        ->assertExitCode(1);
})->with([-1, 0.5, 'invalid', '1e999', true]);
