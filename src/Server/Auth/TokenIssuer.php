<?php

namespace Arttecs\Murmur\Server\Auth;

use Arttecs\Murmur\Server\Topic;
use Closure;
use Firebase\JWT\JWT;
use InvalidArgumentException;
use SensitiveParameter;

/**
 * Issue signed tokens for topic subscriptions.
 *
 * @internal
 */
class TokenIssuer
{
    /**
     * The clock that returns the current time in seconds.
     */
    protected Closure $clock;

    /**
     * Create a new subscription token issuer.
     *
     * @param  (callable(): int)|null  $clock
     *
     * @throws InvalidArgumentException
     */
    public function __construct(#[SensitiveParameter] protected string $secret, ?callable $clock = null)
    {
        Secret::assertValid($secret);

        $this->clock = Closure::fromCallable($clock ?? 'time');
    }

    /**
     * Issue a subscription token for the given topics and lifetime.
     *
     * @param  array<string>  $topics
     *
     * @throws InvalidArgumentException
     */
    public function issue(array $topics, int $ttlSeconds): string
    {
        if (array_filter($topics, fn (mixed $topic): bool => is_string($topic) && Topic::isValid($topic)) !== $topics) {
            throw new InvalidArgumentException('A subscription token must contain valid topic names.');
        }

        $now = ($this->clock)();

        return JWT::encode([
            'topics' => array_values($topics),
            'exp' => $now + $ttlSeconds,
        ], $this->secret, 'HS256');
    }
}
