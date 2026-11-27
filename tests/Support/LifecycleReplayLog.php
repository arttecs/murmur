<?php

namespace Arttecs\Murmur\Tests\Support;

use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use React\Promise\Promise;
use React\Promise\PromiseInterface;
use RuntimeException;

use function React\Promise\reject;

class LifecycleReplayLog extends MemoryEventLog
{
    public function __construct(protected string $mode)
    {
        parent::__construct();
    }

    public function since(EventId $after, array $topics, int $limit, ?callable $filter = null): PromiseInterface
    {
        if ($topics === []) {
            return parent::since($after, $topics, $limit, $filter);
        }

        return match ($this->mode) {
            'error' => reject(new RuntimeException('The test replay failed.')),
            'slow_consumer' => new Promise(static function (): void {}, static function (): void {
                throw new RuntimeException('The test replay was cancelled.');
            }),
        };
    }
}
