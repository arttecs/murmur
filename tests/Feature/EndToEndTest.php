<?php

use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SseReader;

it('delivers a laravel broadcast to a browser subscription end to end', function () {
    $h = ServerHarness::start();
    config(['murmur.publish_url' => $h->url(''), 'murmur.public_url' => $h->url(''), 'murmur.stream_cursor_margin_ms' => 0]);
    $stream = Murmur::stream('private-chat.42');
    $r = SseReader::open($stream->url());
    $r->next();
    publishFromSubprocess($h, 'private-chat.42', 'token', ['text' => 'Olá']);
    $frame = $r->next();
    expect($frame['event'])->toBe('token');
    expect(json_decode($frame['data'], true)['data'])->toBe(['text' => 'Olá']);
});

it('reports an oversized publish as a publish failure', function () {
    $h = ServerHarness::start(['maxPayloadBytes' => 64]);
    $process = runPublishProcess([
        __DIR__.'/../Support/publish-oversized.php',
        $h->url(''),
        config('murmur.secret'),
    ]);

    expect($process->getExitCode())->toBe(0, $process->getErrorOutput());
});
