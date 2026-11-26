<?php

namespace Arttecs\Murmur\Tests\Feature\Conformance;

use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Log\RedisEventLog;
use Arttecs\Murmur\Testing\EventLogConformanceTestCase;
use Clue\React\Redis\Client;
use Clue\React\Redis\Factory;
use React\EventLoop\LoopInterface;
use React\Promise\PromiseInterface;
use Throwable;

class RedisEventLogTest extends EventLogConformanceTestCase
{
    protected ?Client $client = null;

    protected ?string $key = null;

    protected function createEventLog(int $capacity, LoopInterface $loop): array
    {
        $url = getenv('MURMUR_TEST_REDIS_URL') ?: 'redis://127.0.0.1:6379';
        $this->client = (new Factory($loop))->createLazyClient($url);

        try {
            $this->await($this->client->ping());
        } catch (Throwable $error) {
            if (getenv('CI') !== false) {
                throw $error;
            }

            $this->markTestSkipped('Redis is unreachable.');
        }

        $this->key = 'murmur:test:'.bin2hex(random_bytes(16));
        $log = new RedisEventLog($url, $this->key, $capacity, $loop);

        return [$log, fn (Event $event): PromiseInterface => $this->client->xadd(
            $this->key, 'MAXLEN', $capacity, '*', 'topic', $event->topic, 'name', $event->name, 'data',
            json_encode($event->data, JSON_THROW_ON_ERROR),
        )->then(EventId::tryParse(...))];
    }

    protected function tearDown(): void
    {
        try {
            parent::tearDown();
        } finally {
            try {
                if ($this->key !== null) {
                    $this->await($this->client->del($this->key));
                }
            } finally {
                $this->client?->close();
            }
        }
    }
}
