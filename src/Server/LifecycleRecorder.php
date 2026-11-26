<?php

namespace Arttecs\Murmur\Server;

use SplQueue;

/**
 * Buffer connection lifecycle records in order until the worker can accept them.
 *
 * @internal
 */
class LifecycleRecorder
{
    /**
     * The pending records, or null after the worker stops.
     *
     * @var SplQueue<array{connectionId: string, topics: list<string>, occurredAt: int, durationSeconds?: float, reason?: string}>|null
     */
    protected ?SplQueue $records;

    /**
     * Create an empty queue with a fixed record capacity.
     */
    public function __construct(protected int $capacity)
    {
        $this->records = new SplQueue;
    }

    /**
     * Enqueue a record and return the number of records dropped.
     *
     * @param  array{connectionId: string, topics: list<string>, occurredAt: int, durationSeconds?: float, reason?: string}  $record
     */
    public function enqueue(array $record): int
    {
        if ($this->records === null) {
            return 1;
        }

        $full = $this->records->count() === $this->capacity;

        if ($full) {
            $this->records->dequeue();
        }

        $this->records->enqueue($record);

        return (int) $full;
    }

    /**
     * Determine whether at least the given number of records are waiting.
     */
    public function pending(int $minimum = 1): bool
    {
        return ($this->records?->count() ?? 0) >= $minimum;
    }

    /**
     * Take an ordered batch within the worker's count and byte limits.
     *
     * @return array{string, int, int} The encoded batch, record count, and drop count.
     *
     * @throws \JsonException
     */
    public function batch(string $node, int $limit, int $maxBytes): array
    {
        $prefix = '{"type":"lifecycle","node":'.json_encode($node, JSON_THROW_ON_ERROR).',"records":[';
        $suffix = ']}';
        $bytes = strlen($prefix) + strlen($suffix);
        $encoded = [];
        $dropped = 0;

        while ($this->pending() && count($encoded) + $dropped < $limit) {
            $record = $this->records->bottom();
            $record['topics'] = array_map(base64_encode(...), $record['topics']);
            $json = json_encode($record, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION);
            $size = strlen($json) + (int) ($encoded !== []);

            if ($bytes + $size > $maxBytes) {
                if ($encoded !== []) {
                    break;
                }

                $this->records->dequeue();
                $dropped++;

                continue;
            }

            $this->records->dequeue();
            $bytes += $size;
            $encoded[] = $json;
        }

        return [$prefix.implode(',', $encoded).$suffix, count($encoded), $dropped];
    }

    /**
     * Drop outstanding records and refuse future records.
     */
    public function stop(): int
    {
        $dropped = $this->records?->count() ?? 0;
        $this->records = null;

        return $dropped;
    }
}
