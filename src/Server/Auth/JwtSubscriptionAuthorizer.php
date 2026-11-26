<?php

namespace Arttecs\Murmur\Server\Auth;

use Arttecs\Murmur\Contracts\SubscriptionAuthorizer;
use Arttecs\Murmur\Exceptions\InvalidToken;
use Arttecs\Murmur\Server\Topic;
use React\Promise\PromiseInterface;
use SensitiveParameter;

use function React\Promise\resolve;

/**
 * Authorize subscriptions using the shared Murmur JWT secret.
 *
 * @internal
 */
class JwtSubscriptionAuthorizer implements SubscriptionAuthorizer
{
    /**
     * The verifier for the built-in subscription token format.
     */
    protected TokenVerifier $tokens;

    /**
     * Create an authorizer using the server's shared secret.
     */
    public function __construct(#[SensitiveParameter] string $secret)
    {
        $this->tokens = new TokenVerifier($secret);
    }

    /**
     * {@inheritdoc}
     */
    public function authorize(?string $token, array $topics): PromiseInterface
    {
        return resolve(null)->then(function () use ($token, $topics): Grant {
            if ($token !== null) {
                return $this->tokens->verify($token);
            }

            if (array_filter($topics, Topic::isPrivate(...)) !== []) {
                throw new InvalidToken('A private subscription requires a token.');
            }

            return new Grant([], PHP_INT_MAX);
        });
    }
}
