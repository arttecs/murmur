<?php

namespace Arttecs\Murmur\Testing;

use Arttecs\Murmur\Contracts\SubscriptionAuthorizer;
use Arttecs\Murmur\Exceptions\InvalidToken;
use Arttecs\Murmur\Server\Auth\Grant;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function React\Async\await;
use function React\Promise\Timer\timeout;

/**
 * Verify a token-based authorizer against Murmur's subscription contract.
 */
abstract class SubscriptionAuthorizerConformanceTestCase extends TestCase
{
    /**
     * Supply an authorizer and credentials for the test topics.
     *
     * The valid token must cover private-news and private-weather only.
     *
     * @return array{SubscriptionAuthorizer, array{valid: string, garbage: string, expired: string}}
     */
    abstract protected function createAuthorizer(): array;

    /**
     * Resolve the token's exact grant with a future expiry before the test deadline.
     */
    public function test_grants_exactly_the_topics_covered_by_a_valid_token(): void
    {
        [$authorizer, $tokens] = $this->createAuthorizer();
        $grant = await(timeout($authorizer->authorize($tokens['valid'], ['private-news', 'private-weather', 'private-other']), 3.0));

        self::assertInstanceOf(Grant::class, $grant);
        self::assertEqualsCanonicalizing(['private-news', 'private-weather'], $grant->topics);
        self::assertGreaterThan(time(), $grant->expiresAt);
    }

    /**
     * Reject missing, malformed, and expired credentials before the test deadline.
     */
    #[TestWith([null])]
    #[TestWith(['garbage'])]
    #[TestWith(['expired'])]
    public function test_rejects_invalid_tokens(?string $kind): void
    {
        [$authorizer, $tokens] = $this->createAuthorizer();
        $promise = $authorizer->authorize($kind === null ? null : $tokens[$kind], ['private-news']);

        $this->expectException(InvalidToken::class);
        await(timeout($promise, 3.0));
    }
}
