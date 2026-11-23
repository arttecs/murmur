<?php

namespace Arttecs\Murmur;

use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Exceptions\PublishFailed;
use Arttecs\Murmur\Server\Auth\TokenIssuer;
use Arttecs\Murmur\Server\Event;
use Illuminate\Contracts\Config\Repository;
use InvalidArgumentException;

/**
 * Publish events, create streams and issue subscription tokens.
 */
class MurmurManager
{
    /**
     * The subscription token issuer.
     */
    protected TokenIssuer $tokenIssuer;

    /**
     * Create a manager for the configured publisher and token issuer.
     *
     * @throws InvalidArgumentException
     */
    public function __construct(protected Publisher $publisher, protected Repository $config)
    {
        $this->tokenIssuer = new TokenIssuer((string) $config->get('murmur.secret'), fn () => now()->timestamp);
    }

    /**
     * Publish an event to the given topic.
     *
     * @throws InvalidArgumentException
     * @throws PublishFailed
     */
    public function publish(string $topic, string $event, mixed $data = null): void
    {
        $this->publisher->publish(new Event($topic, $event, $data));
    }

    /**
     * Create a stream for the given topic.
     */
    public function stream(string $topic): Stream
    {
        return new Stream($this, $topic, $this->config->get('murmur.stream_cursor_margin_ms'));
    }

    /**
     * Issue a subscription token for the given topics.
     *
     * @param  array<string>  $topics
     */
    public function token(array $topics, ?int $ttl = null): string
    {
        return $this->tokenIssuer->issue($topics, $ttl ?? $this->config->get('murmur.token_ttl'));
    }

    /**
     * Get the public base URL of the Murmur server.
     */
    public function publicUrl(): string
    {
        return rtrim($this->config->get('murmur.public_url'), '/');
    }
}
