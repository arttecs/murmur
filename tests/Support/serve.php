<?php

use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Server\Server;
use Arttecs\Murmur\Server\ServerConfig;
use React\EventLoop\Loop;

use function React\Async\await;

require dirname(__DIR__, 2).'/vendor/autoload.php';

$loop = Loop::get();
$config = new ServerConfig(secret: $argv[2], host: '127.0.0.1', port: (int) $argv[1]);
$server = new Server($config, new MemoryEventLog, $loop);

if (extension_loaded('pcntl')) {
    $loop->addSignal(SIGTERM, static fn () => $server->shutdown()->then($loop->stop(...)));
}

try {
    await($server->listen());
    fwrite(STDOUT, $server->address().PHP_EOL);
    $loop->run();
} finally {
    $server->close();
}
