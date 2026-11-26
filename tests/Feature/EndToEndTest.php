<?php

use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\Tests\Fixtures\OrderShipped;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SseReader;

it('delivers a signed publish to a subscriber of a Laravel-issued stream URL', function () {
    $h = ServerHarness::start();
    config(['murmur.publish_url' => $h->url(''), 'murmur.public_url' => $h->url(''), 'murmur.stream_cursor_margin_ms' => 0]);
    $stream = Murmur::stream('private-chat.42');
    $r = SseReader::open($stream->url());
    $r->start(replaying: true);
    expect($r->next()['event'])->toBe('murmur.gap');
    expect($r->attached()['id'])->toBeNull();
    publishFromSubprocess($h, 'private-chat.42', 'token', ['text' => 'Résumé']);
    $frame = $r->next();
    expect($frame['event'])->toBe('token');
    expect(json_decode($frame['data'], true)['data'])->toBe(['text' => 'Résumé']);
});

it('reports an oversized publish as a publish failure', function () {
    $h = ServerHarness::start(['maxPayloadBytes' => 64]);
    $process = runPublishProcess([
        __DIR__.'/../Support/publish-oversized.php',
        $h->url(''),
        config('murmur.secret'),
    ]);

    expect($process->getExitCode())->toBe(0, $process->getOutput().$process->getErrorOutput());
});

it('delivers a laravel broadcast event to a subscriber end to end', function () {
    $h = ServerHarness::start();
    $topic = 'private-orders.42';
    $query = http_build_query(['topics' => $topic, 'token' => Murmur::token([$topic])]);
    $reader = SseReader::open($h->url('/sse?'.$query));
    $reader->start();

    $process = runPublishProcess([
        __DIR__.'/../Support/broadcast.php',
        $h->url(''),
        config('murmur.secret'),
    ]);
    $output = $process->getOutput().$process->getErrorOutput();
    expect($process->getExitCode())->toBe(0, $output);

    try {
        $frame = $reader->next();
        $envelope = json_decode($frame['data'], true, flags: JSON_THROW_ON_ERROR);
    } catch (Throwable $error) {
        $this->fail($error->getMessage()."\n".$output);
    }

    expect($frame['event'])->toBe(OrderShipped::class, $output);
    expect($envelope)->toBe(['topic' => $topic, 'data' => ['orderId' => 42]], $output);
});

it('delivers a laravel toOthers broadcast only to the other subscriber', function () {
    $h = ServerHarness::start();
    $topic = 'private-orders.42';
    $query = http_build_query(['topics' => $topic, 'token' => Murmur::token([$topic])]);
    $id = str_repeat('a', 32);
    $sender = SseReader::open($h->url('/sse?'.$query.'&connection='.$id));
    $other = SseReader::open($h->url('/sse?'.$query));
    $sender->start();
    $other->start();

    $process = runPublishProcess([
        __DIR__.'/../Support/broadcast.php',
        $h->url(''),
        config('murmur.secret'),
        $id,
    ]);

    expect($process->getExitCode())->toBe(0, $process->getOutput().$process->getErrorOutput());
    $frame = $other->next();
    expect($frame['event'])->toBe(OrderShipped::class);
    expect(json_decode($frame['data'], true))->toBe(['topic' => $topic, 'data' => ['orderId' => 42]]);
    expect(fn () => $sender->next(0.05))->toThrow(RuntimeException::class, 'Timed out');
});
