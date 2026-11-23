<?php

use Arttecs\Murmur\Console\Commands\ServeCommand;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
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
