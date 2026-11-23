<?php

use Arttecs\Murmur\Tests\Support\ServerHarness;

it('rejects bad publishes', function () {
    $h = ServerHarness::start(['maxPayloadBytes' => 64]);
    expect($h->rawPublish('{"topic":"news","event":"x"}', signature: 'bad')->getStatusCode())->toBe(401);
    expect($h->rawPublish('{"topic":"a b","event":"x"}')->getStatusCode())->toBe(422);
    expect($h->rawPublish('{"topic":"news","event":"x\ny"}')->getStatusCode())->toBe(422);
    expect($h->rawPublish('not json')->getStatusCode())->toBe(422);
    expect($h->rawPublish(json_encode(['topic' => 'news', 'event' => 'x', 'data' => str_repeat('x', 100)]))->getStatusCode())->toBe(413);
});
