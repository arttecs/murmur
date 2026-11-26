<?php

namespace Arttecs\Murmur\Metrics;

use stdClass;

/**
 * Convert snapshots between the PHP metrics representation and JSON.
 *
 * @phpstan-import-type Sample from \Arttecs\Murmur\Contracts\MetricsStore
 * @phpstan-import-type Snapshot from \Arttecs\Murmur\Contracts\MetricsStore as SnapshotData
 */
class Snapshot
{
    /**
     * Prepare samples for JSON so even an empty label set encodes as an object.
     *
     * @param  list<Sample>  $samples
     * @return list<array{name: string, type: 'counter'|'gauge', help: string, labels: object, value: int|float}>
     */
    public static function encodeSamples(array $samples): array
    {
        return array_map(fn (array $sample): array => [...$sample, 'labels' => (object) $sample['labels']], $samples);
    }

    /**
     * Check whether a value can be used as a server node name.
     */
    public static function validNode(mixed $node): bool
    {
        return is_string($node) && preg_match('/\A[A-Za-z0-9._:-]{1,128}\z/', $node) === 1;
    }

    /**
     * Validate and normalize decoded JSON.
     *
     * @return SnapshotData|null
     */
    public static function fromDecoded(mixed $recordedAt, mixed $samples): ?array
    {
        if (! is_int($recordedAt) || $recordedAt < 1 || ! is_array($samples) || ! array_is_list($samples)) {
            return null;
        }

        $normalized = [];

        foreach ($samples as $sample) {
            if (! $sample instanceof stdClass || count((array) $sample) !== 5
                || ! is_string($sample->name ?? null)
                || ! in_array($sample->type ?? null, ['counter', 'gauge'], true)
                || ! is_string($sample->help ?? null)
                || ! ($sample->labels ?? null) instanceof stdClass
                || (! is_int($sample->value ?? null) && ! is_float($sample->value ?? null))
                || ! is_finite($sample->value)) {
                return null;
            }

            $labels = (array) $sample->labels;

            foreach ($labels as $label) {
                if (! is_string($label)) {
                    return null;
                }
            }

            $normalized[] = [
                'name' => $sample->name,
                'type' => $sample->type,
                'help' => $sample->help,
                'labels' => $labels,
                'value' => $sample->value,
            ];
        }

        return ['recorded_at' => $recordedAt, 'samples' => $normalized];
    }

    /**
     * Decode a stored snapshot, ignoring malformed entries.
     *
     * @return SnapshotData|null
     */
    public static function decode(mixed $json): ?array
    {
        $value = is_string($json) ? json_decode($json) : null;

        return $value instanceof stdClass && count((array) $value) === 2
            ? static::fromDecoded($value->recorded_at ?? null, $value->samples ?? null)
            : null;
    }

    /**
     * Encode a timestamp and its samples for storage.
     *
     * @param  list<Sample>  $samples
     */
    public static function encode(int $recordedAt, array $samples): string
    {
        return json_encode(['recorded_at' => $recordedAt, 'samples' => static::encodeSamples($samples)], JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION);
    }
}
