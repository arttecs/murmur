<?php

use Arttecs\Murmur\Console\Commands\ServeCommand;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Server\Log\RedisEventLog;
use Symfony\Component\Console\Input\ArrayInput;

use function React\Async\await;

it('maps laravel config and options onto the server config', function () {
    config(['murmur.server.heartbeat' => 5, 'murmur.server.allowed_origins' => ['https://app.test']]);
    $command = app(ServeCommand::class);
    $command->setLaravel(app());
    $command->setInput(new ArrayInput(['--port' => '9999'], $command->getDefinition()));
    $config = $command->makeConfig();
    expect($config->port)->toBe(9999);
    expect($config->host)->toBe('127.0.0.1');
    expect($config->heartbeatSeconds)->toBe(5.0);
    expect($config->allowedOrigins)->toBe(['https://app.test']);
    expect($config->secret)->toBe('test-secret-with-at-least-32-bytes');
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

    expect(await($log->since($first->id, ['news']))->gap)->toBeTrue();
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
        $replay = await($log->since(EventId::tryParse($first), ['news']));
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
