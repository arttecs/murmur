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
     * The server calls this once per distinct token, or once with null when
     * no token was supplied, up to max_topics_per_connection calls per request.
     * Tokens must not contain commas. This method runs in the event loop.
     * Use non-blocking I/O and cache or bound the work per request.
     * Constructing an invalid Grant throws, and the server responds with HTTP 500.
     *
     * @param  list<string>  $topics
     * @return PromiseInterface<Grant>
     *
     * @see InvalidToken
     */
    public function authorize(?string $token, array $topics): PromiseInterface;
}
