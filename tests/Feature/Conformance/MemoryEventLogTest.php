<?php

namespace Arttecs\Murmur\Tests\Feature\Conformance;

use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Testing\EventLogConformanceTestCase;
use React\EventLoop\LoopInterface;

class MemoryEventLogTest extends EventLogConformanceTestCase
{
    protected function createEventLog(int $capacity, LoopInterface $loop): array
    {
        $log = new MemoryEventLog($capacity);

        return [$log, fn (Event $event): EventId => $log->append($event)->id];
    }
}
