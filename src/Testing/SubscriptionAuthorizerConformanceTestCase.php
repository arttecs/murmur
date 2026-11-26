<?php

namespace Arttecs\Murmur\Testing;

use Arttecs\Murmur\Contracts\SubscriptionAuthorizer;
use Arttecs\Murmur\Exceptions\InvalidToken;
use Arttecs\Murmur\Server\Auth\Grant;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

/**
 * Verify a token-based authorizer against Murmur's subscription contract.
 *
 * Handling an omitted token is deployment-defined. Test that policy in the
 * concrete suite when it applies.
 */
abstract class SubscriptionAuthorizerConformanceTestCase extends TestCase
{
    use AwaitsPromises;

    /**
     * Supply an authorizer and credentials for the test topics.
     *
     * Use the test's loop for asynchronous authorization. The valid token must
     * cover private-news and private-weather only.
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
        $grant = $this->await($authorizer->authorize($tokens['valid'], ['private-news', 'private-weather', 'private-other']));

        self::assertInstanceOf(Grant::class, $grant);
        self::assertEqualsCanonicalizing(['private-news', 'private-weather'], $grant->topics);
        self::assertGreaterThan(time(), $grant->expiresAt);
    }

    /**
     * Reject malformed and expired credentials before the test deadline.
     */
    #[TestWith(['garbage'])]
    #[TestWith(['expired'])]
    public function test_rejects_invalid_tokens(string $kind): void
    {
        [$authorizer, $tokens] = $this->createAuthorizer();
        $this->expectException(InvalidToken::class);
        $this->await($authorizer->authorize($tokens[$kind], ['private-news']));
    }
}
