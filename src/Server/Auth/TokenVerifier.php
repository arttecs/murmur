<?php

namespace Arttecs\Murmur\Server\Auth;

use Arttecs\Murmur\Exceptions\InvalidToken;
use Closure;
use Firebase\JWT\JWT;
use Firebase\JWT\Key;
use InvalidArgumentException;
use Throwable;

/**
 * Verify subscription tokens and read their grants.
 */
class TokenVerifier
{
    /**
     * The clock that returns the current time in seconds.
     */
    protected Closure $clock;

    /**
     * Create a new subscription token verifier.
     *
     * @param  (callable(): int)|null  $clock
     *
     * @throws InvalidArgumentException
     */
    public function __construct(protected string $secret, ?callable $clock = null)
    {
        Secret::assertValid($secret);

        $this->clock = Closure::fromCallable($clock ?? 'time');
    }

    /**
     * Verify a subscription token and return its grant.
     *
     * @throws InvalidToken
     */
    public function verify(string $jwt): Grant
    {
        $now = ($this->clock)();
        $previousTimestamp = JWT::$timestamp;

        try {
            JWT::$timestamp = $now;
            $claims = JWT::decode($jwt, new Key($this->secret, 'HS256'));
        } catch (Throwable $exception) {
            throw new InvalidToken('Invalid subscription token.', 0, $exception);
        } finally {
            JWT::$timestamp = $previousTimestamp;
        }

        $topics = $claims->topics ?? null;
        $expiresAt = $claims->exp ?? null;

        if (! is_array($topics) || ! array_is_list($topics) || array_filter($topics, 'is_string') !== $topics) {
            throw new InvalidToken('The subscription token must contain a list of topic strings.');
        }

        if (! is_int($expiresAt)) {
            throw new InvalidToken('The subscription token must contain an integer expiration timestamp.');
        }

        return new Grant($topics, $expiresAt);
    }
}
