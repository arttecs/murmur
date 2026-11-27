<?php

use Arttecs\Murmur\Exceptions\PublishFailed;
use Arttecs\Murmur\Publishers\HttpPublisher;
use Arttecs\Murmur\Server\Event;
use Illuminate\Http\Client\Factory;

require __DIR__.'/../../vendor/autoload.php';

[, $url, $secret] = $argv;
$publisher = new HttpPublisher(new Factory, $url, $secret);

try {
    $publisher->publish(new Event('news', 'updated', str_repeat('x', 100)));
} catch (PublishFailed $exception) {
    if (str_ends_with($exception->getMessage(), 'HTTP status 413.')) {
        exit(0);
    }

    throw $exception;
}

fwrite(STDERR, "The oversized publish did not throw PublishFailed.\n");
exit(1);
