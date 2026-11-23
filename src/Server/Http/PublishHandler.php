<?php

namespace Arttecs\Murmur\Server\Http;

use Arttecs\Murmur\Server\Auth\Signature;
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
     * Verify the raw body and append a valid event.
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
            || ! is_string($payload->topic ?? null) || ! Topic::isValid($payload->topic)
            || ! is_string($payload->event ?? null) || ! Event::isValidName($payload->event)) {
            return new Response(422);
        }

        $data = $payload->data ?? null;

        if (json_encode($data) === false) {
            return new Response(422);
        }

        $event = $this->log->append(new Event($payload->topic, $payload->event, $data));

        return new Response(202, ['Content-Type' => 'application/json'], json_encode([
            'id' => (string) $event->id,
        ], JSON_THROW_ON_ERROR));
    }
}
