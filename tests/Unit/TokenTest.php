<?php

use Arttecs\Murmur\Exceptions\InvalidToken;
use Arttecs\Murmur\Server\Auth\Signature;
use Arttecs\Murmur\Server\Auth\TokenIssuer;
use Arttecs\Murmur\Server\Auth\TokenVerifier;
use Firebase\JWT\JWT;

it('round-trips a grant', function () {
    $jwt = (new TokenIssuer('s3cret-s3cret-s3cret-s3cret-s3cret', fn () => 1000))->issue(['private-a', 'private-b'], 900);
    $grant = (new TokenVerifier('s3cret-s3cret-s3cret-s3cret-s3cret', fn () => 1001))->verify($jwt);
    expect($grant->topics)->toBe(['private-a', 'private-b']);
    expect($grant->expiresAt)->toBe(1900);
    expect($grant->allows('private-a'))->toBeTrue();
    expect($grant->allows('private-c'))->toBeFalse();
});

it('rejects an expired token', function () {
    $jwt = (new TokenIssuer('s3cret-s3cret-s3cret-s3cret-s3cret', fn () => 1000))->issue(['private-a'], 900);
    (new TokenVerifier('s3cret-s3cret-s3cret-s3cret-s3cret', fn () => 1901))->verify($jwt);
})->throws(InvalidToken::class);

it('rejects a token signed with another secret', function () {
    $jwt = (new TokenIssuer('other-other-other-other-other-other', fn () => 1000))->issue(['private-a'], 900);
    (new TokenVerifier('s3cret-s3cret-s3cret-s3cret-s3cret', fn () => 1001))->verify($jwt);
})->throws(InvalidToken::class);

it('rejects garbage', function () {
    (new TokenVerifier('s3cret-s3cret-s3cret-s3cret-s3cret'))->verify('not-a-jwt');
})->throws(InvalidToken::class);

it('rejects secrets shorter than 32 bytes', function () {
    expect(fn () => new TokenIssuer('short'))->toThrow(InvalidArgumentException::class, 'at least 32 bytes');
    expect(fn () => new TokenVerifier('short'))->toThrow(InvalidArgumentException::class, 'at least 32 bytes');
    expect(fn () => Signature::sign('short', 1000, '{}'))->toThrow(InvalidArgumentException::class, 'at least 32 bytes');
});

it('rejects a token signed with another algorithm', function () {
    $jwt = JWT::encode(['topics' => ['private-a'], 'exp' => 1900], str_repeat('k', 64), 'HS512');
    (new TokenVerifier(str_repeat('k', 64), fn () => 1000))->verify($jwt);
})->throws(InvalidToken::class);

it('rejects a token without a valid list of topics', function (array $claims) {
    $jwt = JWT::encode($claims + ['exp' => 1900], 's3cret-s3cret-s3cret-s3cret-s3cret', 'HS256');
    (new TokenVerifier('s3cret-s3cret-s3cret-s3cret-s3cret', fn () => 1000))->verify($jwt);
})->with([
    'missing' => [[]],
    'map' => [['topics' => ['a' => 'x']]],
    'non-string' => [['topics' => [1]]],
])->throws(InvalidToken::class);

it('rejects a token without an expiration', function () {
    $jwt = JWT::encode(['topics' => ['private-a']], 's3cret-s3cret-s3cret-s3cret-s3cret', 'HS256');
    (new TokenVerifier('s3cret-s3cret-s3cret-s3cret-s3cret', fn () => 1000))->verify($jwt);
})->throws(InvalidToken::class);

it('ignores and restores the global jwt clock and leeway', function () {
    JWT::$timestamp = 5;
    JWT::$leeway = 3600;

    try {
        $jwt = (new TokenIssuer('s3cret-s3cret-s3cret-s3cret-s3cret', fn () => 1000))->issue(['private-a'], 900);
        $verifier = new TokenVerifier('s3cret-s3cret-s3cret-s3cret-s3cret', fn () => 1901);

        expect(fn () => $verifier->verify($jwt))->toThrow(InvalidToken::class);
        expect(JWT::$timestamp)->toBe(5);
        expect(JWT::$leeway)->toBe(3600);
    } finally {
        JWT::$timestamp = null;
        JWT::$leeway = 0;
    }
});
