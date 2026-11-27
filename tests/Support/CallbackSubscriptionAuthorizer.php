<?php

namespace Arttecs\Murmur\Tests\Support;

use Arttecs\Murmur\Contracts\SubscriptionAuthorizer;
use Closure;
use React\Promise\PromiseInterface;

/**
 * Authorize subscriptions using a callback supplied by a test.
 */
class CallbackSubscriptionAuthorizer implements SubscriptionAuthorizer
{
    public function __construct(protected Closure $callback) {}

    public function authorize(?string $token, array $topics): PromiseInterface
    {
        return ($this->callback)($token, $topics);
    }
}
