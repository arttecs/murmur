<?php

namespace Arttecs\Murmur\Publishers;

use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Contracts\PublishesToManyTopics;
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
class RedisPublisher implements Publisher, PublishesToManyTopics
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
            $data = $this->encodeData($event->data);

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

    /**
     * Append one stream entry per topic in a single Redis operation.
     *
     * The script runs atomically, so no other command interleaves with the batch. Redis
     * cannot roll back a script, so an error raised by a later entry leaves the earlier
     * entries in the stream.
     *
     * @param  list<string>  $topics
     *
     * @throws PublishFailed
     */
    public function publishToTopics(array $topics, string $name, mixed $data = null, ?string $except = null): void
    {
        try {
            $data = $this->encodeData($data);
            $connection = $this->redis->connection($this->connection);
            $script = <<<'LUA'
                local ids = {}
                for i = 5, #ARGV do
                    local fields = {'topic', ARGV[i], 'name', ARGV[2], 'data', ARGV[3]}
                    if ARGV[4] ~= '' then
                        fields[#fields + 1] = 'except'
                        fields[#fields + 1] = ARGV[4]
                    end
                    ids[#ids + 1] = redis.call('XADD', KEYS[1], 'MAXLEN', '~', ARGV[1], '*', unpack(fields))
                end
                return ids
                LUA;
            $ids = $connection->executeRaw([
                'EVAL', $script, '1', $this->stream,
                $this->maxLength, $name, $data, $except ?? '', ...$topics,
            ]);

            if (! is_array($ids) || count($ids) !== count($topics)) {
                $error = $connection instanceof PhpRedisConnection ? $connection->getLastError() : $ids;

                throw new RuntimeException(is_string($error) ? $error : 'Redis did not return the expected stream entry IDs.');
            }

            foreach ($ids as $id) {
                if (! is_string($id) || EventId::tryParse($id) === null) {
                    throw new RuntimeException('Redis did not return a stream entry ID.');
                }
            }
        } catch (PublishFailed $exception) {
            throw $exception;
        } catch (Throwable $exception) {
            throw new PublishFailed('The publish request to Redis failed.', 0, $exception);
        }
    }

    /**
     * Encode the shared stream entry data within the configured size limit.
     *
     * @throws PublishFailed
     * @throws \JsonException
     */
    protected function encodeData(mixed $data): string
    {
        $encoded = json_encode($data, JSON_THROW_ON_ERROR);

        if (strlen($encoded) > $this->maxPayloadBytes) {
            throw new PublishFailed('The Redis event payload exceeds the configured size limit.');
        }

        return $encoded;
    }
}
