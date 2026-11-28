<?php

namespace Arttecs\Murmur\Server\Http;

use Arttecs\Murmur\Server\Auth\Signature;
use Arttecs\Murmur\Server\ConnectionId;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Server\ServerConfig;
use Arttecs\Murmur\Server\Topic;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use React\Http\Message\Response;
use React\Promise\Deferred;
use React\Promise\PromiseInterface;
use React\Stream\ReadableStreamInterface;
use stdClass;

/**
 * Accept signed publish requests and append their events to the in-memory log.
 *
 * @internal
 */
class PublishHandler
{
    /**
     * Create a handler for the server's in-memory log.
     */
    public function __construct(protected ServerConfig $config, protected MemoryEventLog $log) {}

    /**
     * Read the request body within the configured limit and validate its signature.
     *
     * @return ResponseInterface|PromiseInterface<ResponseInterface>
     */
    public function __invoke(ServerRequestInterface $request): ResponseInterface|PromiseInterface
    {
        $timestamp = $request->getHeaderLine('X-Murmur-Timestamp');
        $signature = $request->getHeaderLine('X-Murmur-Signature');

        if (preg_match('/\A[0-9]+\z/', $timestamp) !== 1 || $signature === '') {
            return new Response(401, ['Connection' => 'close']);
        }

        $body = $request->getBody();

        if ($body->getSize() > $this->config->maxPayloadBytes) {
            return new Response(413, ['Connection' => 'close']);
        }

        if (! $body instanceof ReadableStreamInterface) {
            return $this->publish((string) $body, (int) $timestamp, $signature);
        }

        $result = new Deferred;
        $buffer = '';

        $body->on('data', function (string $chunk) use (&$buffer, $result): void {
            if ($buffer === null) {
                return;
            }

            if (strlen($buffer) + strlen($chunk) > $this->config->maxPayloadBytes) {
                $buffer = null;
                $result->resolve(new Response(413, ['Connection' => 'close']));

                return;
            }

            $buffer .= $chunk;
        });

        $body->on('end', function () use (&$buffer, $result): void {
            $result->resolve($buffer);
        });

        $body->on('close', fn () => $result->resolve(null));

        return $result->promise()->then(function (string|ResponseInterface|null $body) use ($timestamp, $signature): ResponseInterface {
            return match (true) {
                $body === null => new Response(422, ['Connection' => 'close']),
                is_string($body) => $this->publish($body, (int) $timestamp, $signature),
                default => $body,
            };
        });
    }

    /**
     * Verify the raw body and append events after validating the entire payload.
     */
    protected function publish(string $body, int $timestamp, string $signature): ResponseInterface
    {
        if (strlen($body) > $this->config->maxPayloadBytes) {
            return new Response(413);
        }

        if (! Signature::verify($this->config->secret, $timestamp, $body, $signature, time(), $this->config->signatureToleranceSeconds)) {
            return new Response(401);
        }

        $payload = json_decode($body);

        if (! $payload instanceof stdClass
            || property_exists($payload, 'topic') === property_exists($payload, 'topics')
            || ! is_string($payload->event ?? null) || ! Event::isPublishableName($payload->event)
            || (property_exists($payload, 'except') && (! is_string($payload->except) || ! ConnectionId::isValid($payload->except)))) {
            return $this->invalid();
        }

        $data = $payload->data ?? null;

        if (json_encode($data) === false) {
            return $this->invalid();
        }

        $batch = property_exists($payload, 'topics');
        $topics = $batch ? $payload->topics : [$payload->topic];

        if (! is_array($topics) || $topics === [] || count($topics) > $this->config->maxTopicsPerPublish) {
            return $this->invalid();
        }

        $events = [];

        foreach ($topics as $topic) {
            if (! is_string($topic) || ! Topic::isValid($topic) || isset($events[$topic])) {
                return $this->invalid();
            }

            $events[$topic] = new Event($topic, $payload->event, $data, except: $payload->except ?? null);
        }

        $ids = [];

        foreach ($events as $event) {
            $ids[] = (string) $this->log->append($event)->id;
        }

        return new Response(202, ['Content-Type' => 'application/json'], json_encode(
            $batch ? ['ids' => $ids] : ['id' => $ids[0]],
            JSON_THROW_ON_ERROR,
        ));
    }

    /**
     * Reject a payload and advertise multi-topic support and its limit.
     *
     * The header lets a publisher tell this rejection from an older server that
     * does not understand the topics field.
     */
    protected function invalid(): ResponseInterface
    {
        return new Response(422, ['X-Murmur-Publish-Topics' => (string) $this->config->maxTopicsPerPublish]);
    }
}
