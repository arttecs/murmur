<?php

namespace Arttecs\Murmur\Publishers;

use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Exceptions\PublishFailed;
use Arttecs\Murmur\Server\Auth\Signature;
use Arttecs\Murmur\Server\Event;
use Illuminate\Http\Client\ConnectionException;
use Illuminate\Http\Client\Factory;
use InvalidArgumentException;
use JsonException;
use SensitiveParameter;

/**
 * Publish signed events over HTTP.
 */
class HttpPublisher implements Publisher
{
    /**
     * Create a publisher for the configured server URL.
     */
    public function __construct(
        protected Factory $http,
        protected string $publishUrl,
        #[SensitiveParameter] protected string $secret,
        protected float $timeout = 2.0,
        protected float $connectTimeout = 1.0,
    ) {}

    /**
     * Sign and send the given event to the publish endpoint.
     *
     * @throws InvalidArgumentException
     * @throws JsonException
     * @throws PublishFailed
     */
    public function publish(Event $event): void
    {
        $body = json_encode([
            'topic' => $event->topic,
            'event' => $event->name,
            'data' => $event->data,
        ], JSON_THROW_ON_ERROR);
        $timestamp = time();

        try {
            $response = $this->http
                ->withHeaders([
                    'X-Murmur-Timestamp' => (string) $timestamp,
                    'X-Murmur-Signature' => Signature::sign($this->secret, $timestamp, $body),
                ])
                ->withBody($body)
                ->timeout($this->timeout)
                ->connectTimeout($this->connectTimeout)
                ->withoutRedirecting()
                ->post(rtrim($this->publishUrl, '/').'/publish');
        } catch (ConnectionException $exception) {
            throw new PublishFailed('Could not connect to the Murmur server.', 0, $exception);
        }

        if ($response->status() !== 202) {
            throw new PublishFailed("The Murmur server rejected the publish request with HTTP status {$response->status()}.");
        }
    }
}
