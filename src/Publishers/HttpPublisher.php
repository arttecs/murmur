<?php

namespace Arttecs\Murmur\Publishers;

use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Contracts\PublishesToManyTopics;
use Arttecs\Murmur\Exceptions\PublishFailed;
use Arttecs\Murmur\Server\Auth\Signature;
use Arttecs\Murmur\Server\Event;
use Illuminate\Http\Client\Factory;
use Illuminate\Http\Client\HttpClientException;
use Illuminate\Http\Client\Response;
use Illuminate\Support\Carbon;
use InvalidArgumentException;
use JsonException;
use SensitiveParameter;

/**
 * Publish signed events over HTTP.
 *
 * @internal
 */
class HttpPublisher implements Publisher, PublishesToManyTopics
{
    /**
     * The seconds to keep sending one request per topic after an older server rejects a batch.
     */
    protected const DOWNGRADE_SECONDS = 60;

    /**
     * The Unix time until which batches are sent one topic at a time.
     */
    protected int $downgradedUntil = 0;

    /**
     * Create a publisher for the configured server URL.
     */
    public function __construct(
        protected Factory $http,
        protected string $publishUrl,
        #[SensitiveParameter] protected string $secret,
        protected float $timeout = 2.0,
        protected float $connectTimeout = 1.0,
        protected int $maxTopicsPerPublish = 100,
    ) {}

    /**
     * Sign and send the given event to the publish endpoint.
     *
     * @throws InvalidArgumentException
     * @throws PublishFailed
     */
    public function publish(Event $event): void
    {
        $this->assertAccepted($this->send(['topic' => $event->topic], $event->name, $event->data, $event->except));
    }

    /**
     * Send the topics in as few requests as the server accepts.
     *
     * Each request is validated by the server as a whole. A list longer than the topic
     * limit, or one whose request exceeds the payload limit, is split into several
     * requests, and an earlier request stays published when a later one fails.
     *
     * @param  list<string>  $topics
     *
     * @throws PublishFailed
     */
    public function publishToTopics(array $topics, string $name, mixed $data = null, ?string $except = null): void
    {
        foreach (array_chunk($topics, max(1, $this->maxTopicsPerPublish)) as $chunk) {
            $this->publishChunk($chunk, $name, $data, $except);
        }
    }

    /**
     * Send one request for the topics, splitting or downgrading when the server asks for it.
     *
     * A server that supports the topics field marks its rejections with a header that
     * carries its topic limit. An unmarked 422 comes from an older server, which gets
     * one request per topic until the downgrade expires and the batch is tried again.
     *
     * @param  list<string>  $topics
     *
     * @throws PublishFailed
     */
    protected function publishChunk(array $topics, string $name, mixed $data, ?string $except): void
    {
        if (count($topics) === 1 || Carbon::now()->getTimestamp() < $this->downgradedUntil) {
            foreach ($topics as $topic) {
                $this->publish(new Event($topic, $name, $data, except: $except));
            }

            return;
        }

        $response = $this->send(['topics' => $topics], $name, $data, $except);
        $status = $response->status();
        $limit = (int) $response->header('X-Murmur-Publish-Topics');

        if ($status === 422 && ! $response->hasHeader('X-Murmur-Publish-Topics')) {
            $this->downgradedUntil = Carbon::now()->getTimestamp() + static::DOWNGRADE_SECONDS;
            $this->publishChunk($topics, $name, $data, $except);

            return;
        }

        if ($status === 422 && $limit >= 1 && count($topics) > $limit) {
            $this->maxTopicsPerPublish = $limit;
            $this->publishToTopics($topics, $name, $data, $except);

            return;
        }

        if ($status === 413) {
            foreach (array_chunk($topics, (int) ceil(count($topics) / 2)) as $half) {
                $this->publishChunk($half, $name, $data, $except);
            }

            return;
        }

        $this->assertAccepted($response);
    }

    /**
     * Sign and send a publish request and return its response.
     *
     * @param  array{topic: string}|array{topics: list<string>}  $destination
     *
     * @throws PublishFailed
     */
    protected function send(array $destination, string $name, mixed $data, ?string $except): Response
    {
        try {
            $body = json_encode([
                ...$destination,
                'event' => $name,
                'data' => $data,
                ...($except === null ? [] : ['except' => $except]),
            ], JSON_THROW_ON_ERROR);
            $timestamp = time();

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
        } catch (HttpClientException|JsonException $exception) {
            throw new PublishFailed('The publish request to the Murmur server failed.', 0, $exception);
        }

        return $response;
    }

    /**
     * Throw if the server did not accept the publish request.
     *
     * @throws PublishFailed
     */
    protected function assertAccepted(Response $response): void
    {
        if ($response->status() !== 202) {
            throw new PublishFailed("The Murmur server rejected the publish request with HTTP status {$response->status()}.");
        }
    }
}
