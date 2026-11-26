<?php

namespace Arttecs\Murmur\Contracts;

use Arttecs\Murmur\Exceptions\InvalidToken;
use Arttecs\Murmur\Server\Auth\Grant;
use React\Promise\PromiseInterface;

/**
 * Verify credentials for SSE subscriptions.
 */
interface SubscriptionAuthorizer
{
    /**
     * Resolve a grant for a token or reject with InvalidToken if it is invalid.
     *
     * The server calls this once per supplied token, or once with null when no
     * token was supplied. Tokens must not contain commas. This method runs
     * in the event loop and must not block.
     *
     * @param  list<string>  $topics
     * @return PromiseInterface<Grant>
     *
     * @see InvalidToken
     */
    public function authorize(?string $token, array $topics): PromiseInterface;
}
