<?php

namespace Arttecs\Murmur\Tests\Feature\Conformance;

use Arttecs\Murmur\Server\Auth\JwtSubscriptionAuthorizer;
use Arttecs\Murmur\Server\Auth\TokenIssuer;
use Arttecs\Murmur\Testing\SubscriptionAuthorizerConformanceTestCase;

class JwtSubscriptionAuthorizerTest extends SubscriptionAuthorizerConformanceTestCase
{
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
