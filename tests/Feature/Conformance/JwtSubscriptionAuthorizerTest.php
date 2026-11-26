<?php

namespace Arttecs\Murmur\Tests\Feature\Conformance;

use Arttecs\Murmur\Exceptions\InvalidToken;
use Arttecs\Murmur\Server\Auth\JwtSubscriptionAuthorizer;
use Arttecs\Murmur\Server\Auth\TokenIssuer;
use Arttecs\Murmur\Testing\SubscriptionAuthorizerConformanceTestCase;

class JwtSubscriptionAuthorizerTest extends SubscriptionAuthorizerConformanceTestCase
{
    /**
     * The default JWT authorizer requires a token for private topics.
     */
    public function test_rejects_a_missing_private_token(): void
    {
        [$authorizer] = $this->createAuthorizer();

        $this->expectException(InvalidToken::class);
        $this->await($authorizer->authorize(null, ['private-news']));
    }

    protected function createAuthorizer(): array
    {
        $secret = 'test-secret-with-at-least-32-bytes';
        $issuer = new TokenIssuer($secret);
        $topics = ['private-news', 'private-weather'];

        return [new JwtSubscriptionAuthorizer($secret), [
            'valid' => $issuer->issue($topics, 60),
            'garbage' => 'not-a-jwt',
            'expired' => $issuer->issue($topics, -1),
        ]];
    }
}
