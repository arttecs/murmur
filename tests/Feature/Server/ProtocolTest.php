<?php

use Arttecs\Murmur\Server\Server;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SseReader;
use React\Http\Browser;

use function React\Async\await;

it('advertises protocol 1 on subscription and probe responses', function () {
    $h = ServerHarness::start();
    $browser = (new Browser)->withRejectErrorResponse(false);
    $subscription = SseReader::open($h->url('/sse?topics=news'));

    expect($subscription->header('X-Murmur-Protocol'))->toBe('1');
    expect(await($browser->get($h->url('/sse')))->getHeaderLine('X-Murmur-Protocol'))->toBe('1');
    expect(await($browser->get($h->url('/ready')))->getHeaderLine('X-Murmur-Protocol'))->toBe('1');
    expect(await($browser->get($h->url('/health')))->getHeaderLine('X-Murmur-Protocol'))->toBe('1');
    expect($h->publish('news', 'updated')->hasHeader('X-Murmur-Protocol'))->toBeFalse();

    $h->log->close();
    expect(await($browser->get($h->url('/ready')))->getHeaderLine('X-Murmur-Protocol'))->toBe('1');
    expect(Server::PROTOCOL)->toBe(1);
});
