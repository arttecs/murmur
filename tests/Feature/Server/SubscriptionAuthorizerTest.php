<?php

use Arttecs\Murmur\Exceptions\InvalidToken;
use Arttecs\Murmur\Server\Auth\Grant;
use Arttecs\Murmur\Server\Http\SseHandler;
use Arttecs\Murmur\Server\Hub;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Server\ServerConfig;
use Arttecs\Murmur\Tests\Support\CallbackSubscriptionAuthorizer;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SseReader;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\Http\Message\ServerRequest;
use React\Promise\Deferred;
use React\Promise\PromiseInterface;
use React\Socket\ConnectionInterface;

use function React\Async\async;
use function React\Async\await;
use function React\Promise\reject;
use function React\Promise\resolve;
use function React\Promise\Timer\sleep;
use function React\Promise\Timer\timeout;

it('authorizes each distinct opaque token once and combines the asynchronous grants', function () {
    $calls = [];
    $authorizer = new CallbackSubscriptionAuthorizer(function (?string $token, array $topics) use (&$calls): PromiseInterface {
        $calls[] = [$token, $topics];

        return sleep(0.01)->then(fn () => new Grant(match ($token) {
            'opaque-a' => ['private-a'],
            'opaque-b' => ['private-b'],
        }, time() + 30));
    });
    $h = ServerHarness::start(authorizer: $authorizer);
    $r = SseReader::open($h->url('/sse?topics=private-a,private-b&token=opaque-a,opaque-b,opaque-a'));

    expect($r->status())->toBe(200);
    $r->start();
    $h->publish('private-b', 'visible');
    expect($r->next()['event'])->toBe('visible');
    expect($calls)->toBe([
        ['opaque-a', ['private-a', 'private-b']],
        ['opaque-b', ['private-a', 'private-b']],
    ]);
    expect(SseReader::open($h->url('/sse?topics=private-a,private-b&token=opaque-a'))->status())->toBe(403);
});

it('passes null once when the token parameter is absent', function () {
    $calls = [];
    $authorizer = new CallbackSubscriptionAuthorizer(function (?string $token, array $topics) use (&$calls): PromiseInterface {
        $calls[] = [$token, $topics];

        return resolve(new Grant(['private-a'], time() + 30));
    });
    $h = ServerHarness::start(authorizer: $authorizer);
    expect(SseReader::open($h->url('/sse?topics=private-a'))->status())->toBe(200);
    expect($calls)->toBe([[null, ['private-a']]]);
});

it('answers 401 when the authorizer throws or rejects with InvalidToken', function (bool $throws) {
    $authorizer = new CallbackSubscriptionAuthorizer(function () use ($throws): PromiseInterface {
        $error = new InvalidToken('Unknown token.');

        return $throws ? throw $error : reject($error);
    });
    $h = ServerHarness::start(authorizer: $authorizer);

    expect(SseReader::open($h->url('/sse?topics=private-a&token=opaque'))->status())->toBe(401);
    expect($h->metrics()->value('murmur_errors_total'))->toBe(0);
})->with([false, true]);

it('reports an unexpected authorizer error and answers 500 without stopping the server', function (bool $throws) {
    $errorLog = tempnam(sys_get_temp_dir(), 'murmur');
    $previous = ini_set('error_log', $errorLog);

    try {
        $authorizer = new CallbackSubscriptionAuthorizer(function () use ($throws): PromiseInterface {
            $error = new RuntimeException('Unexpected subscription error.');

            return $throws ? throw $error : sleep(0.01)->then(fn () => throw $error);
        });
        $h = ServerHarness::start(['allowedOrigins' => ['https://app.test']], authorizer: $authorizer);
        $r = SseReader::open($h->url('/sse?topics=private-a&token=opaque'), ['Origin' => 'https://app.test']);

        expect($r->status())->toBe(500);
        expect($r->header('Access-Control-Allow-Origin'))->toBe('https://app.test');
        expect($h->metrics()->value('murmur_errors_total'))->toBe(1);
        expect($h->pollHealth(0)->getStatusCode())->toBe(200);
    } finally {
        ini_set('error_log', (string) $previous);
        unlink($errorLog);
    }
})->with([false, true]);

it('bounds unsettled authorization by the request deadline and ignores a late grant', function () {
    $pending = new Deferred;
    $calls = 0;
    $authorizer = new CallbackSubscriptionAuthorizer(function () use ($pending, &$calls): PromiseInterface {
        $calls++;

        return $pending->promise();
    });
    $h = ServerHarness::start(['requestTimeoutSeconds' => 0.05], authorizer: $authorizer);
    $request = async(fn () => SseReader::open($h->url('/sse?topics=private-a&token=opaque')))();

    expect(fn () => await(timeout($request, 1.0)))->toThrow(RuntimeException::class, 'ended before receiving response');
    expect($calls)->toBe(1);
    expect($h->trackedSocketCount())->toBe(0);

    $pending->resolve(new Grant(['private-a'], time() + 30));
    expect($h->pollHealth(0)->getStatusCode())->toBe(200);
    expect($h->metrics()->value('murmur_connections_opened_total'))->toBe(0);
    expect($h->metrics()->value('murmur_errors_total'))->toBe(0);
});

it('closes a subscription when the custom grant expires', function () {
    $authorizer = new CallbackSubscriptionAuthorizer(fn (): PromiseInterface => resolve(new Grant(['private-a'], time() + 2)));
    $h = ServerHarness::start(authorizer: $authorizer);
    $r = SseReader::open($h->url('/sse?topics=private-a&token=opaque'));
    expect($r->status())->toBe(200);
    $r->start();
    expect(fn () => $r->next(3.0))->toThrow(RuntimeException::class, 'closed');
});

it('refuses an already expired custom grant', function () {
    $authorizer = new CallbackSubscriptionAuthorizer(fn (): PromiseInterface => resolve(new Grant(['private-a'], time() - 1)));
    $h = ServerHarness::start(authorizer: $authorizer);

    expect(SseReader::open($h->url('/sse?topics=private-a&token=opaque'))->status())->toBe(401);
});

it('opens a private subscription without a timer for an expiry beyond the horizon', function () {
    $loop = $this->createMock(LoopInterface::class);
    $loop->expects($this->never())->method('addTimer');
    $handler = new SseHandler(
        new ServerConfig(str_repeat('x', 32)),
        new MemoryEventLog,
        new Hub,
        $loop,
        new CallbackSubscriptionAuthorizer(fn () => resolve(new Grant(['private-a'], PHP_INT_MAX))),
        fn () => true,
    );
    $client = $this->createStub(ConnectionInterface::class);
    $request = new ServerRequest('GET', '/sse', [], '');
    $response = await($handler($request->withQueryParams(['topics' => 'private-a']), $client));

    expect($response->getStatusCode())->toBe(200);
    $response->getBody()->close();
});

it('cancels pending authorization when the client closes the socket first', function () {
    $started = new Deferred;
    $cancelled = new Deferred;
    $pending = new Deferred(function () use ($cancelled): never {
        Loop::futureTick(fn () => $cancelled->resolve(null));

        throw new RuntimeException('Authorization was cancelled.');
    });
    $authorizer = new CallbackSubscriptionAuthorizer(function () use ($started, $pending): PromiseInterface {
        $started->resolve(null);

        return $pending->promise();
    });
    $h = ServerHarness::start(authorizer: $authorizer);
    $client = $h->connect();

    try {
        $client->write("GET /sse?topics=private-a&token=opaque HTTP/1.1\r\nHost: localhost\r\n\r\n");
        await(timeout($started->promise(), 1.0));
        expect($h->metrics()->value('murmur_sockets'))->toBe(1);
        $client->close();
        await(timeout($cancelled->promise(), 1.0));

        expect($h->metrics()->value('murmur_sockets'))->toBe(0);
        expect($h->metrics()->value('murmur_connections_opened_total'))->toBe(0);
    } finally {
        $client->close();
    }
});
