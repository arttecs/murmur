<?php

namespace Arttecs\Murmur\Publishers;

use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Exceptions\PublishFailed;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Illuminate\Contracts\Redis\Factory;
use Illuminate\Redis\Connections\PhpRedisConnection;
use RuntimeException;
use Throwable;

/**
 * Publish events to a Redis stream.
 *
 * @internal
 */
class RedisPublisher implements Publisher
{
    /**
     * Create a publisher for the given connection and fully prefixed stream key.
     */
    public function __construct(
        protected Factory $redis,
        protected string $connection,
        protected string $stream,
        protected int $maxLength,
        protected int $maxPayloadBytes,
    ) {}

    /**
     * Append the given event to the Redis stream.
     *
     * @throws PublishFailed
     */
    public function publish(Event $event): void
    {
        try {
            $data = json_encode($event->data, JSON_THROW_ON_ERROR);

            if (strlen($data) > $this->maxPayloadBytes) {
                throw new PublishFailed('The Redis event payload exceeds the configured size limit.');
            }

            $connection = $this->redis->connection($this->connection);
            $id = $connection->executeRaw([
                'XADD', $this->stream, 'MAXLEN', '~', $this->maxLength, '*',
                'topic', $event->topic, 'name', $event->name, 'data', $data,
                ...($event->except === null ? [] : ['except', $event->except]),
            ]);

            if (! is_string($id) || EventId::tryParse($id) === null) {
                $error = $connection instanceof PhpRedisConnection ? $connection->getLastError() : $id;

                throw new RuntimeException(is_string($error) ? $error : 'Redis did not return a stream entry ID.');
            }
        } catch (PublishFailed $exception) {
            throw $exception;
        } catch (Throwable $exception) {
            throw new PublishFailed('The publish request to Redis failed.', 0, $exception);
        }
    }
}
