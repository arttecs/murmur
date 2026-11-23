<?php

use Arttecs\Murmur\Console\Commands\ServeCommand;
use Symfony\Component\Console\Input\ArrayInput;

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
