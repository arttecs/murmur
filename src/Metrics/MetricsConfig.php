<?php

namespace Arttecs\Murmur\Metrics;

use Arttecs\Murmur\Server\Metrics;
use Illuminate\Contracts\Config\Repository;
use InvalidArgumentException;

/**
 * Validate the settings shared by the metrics writer and readers.
 */
class MetricsConfig
{
    /**
     * Hold the validated settings for the selected store.
     *
     * @param  list<string>  $only
     */
    protected function __construct(
        public readonly string $store,
        public readonly float $interval,
        public readonly float $ttl,
        public readonly float $timeout,
        public readonly ?string $node,
        public readonly array $only,
        public readonly string $table,
    ) {}

    /**
     * Read the metrics configuration for an enabled store.
     *
     * @throws InvalidArgumentException
     */
    public static function from(Repository $config, string $store): static
    {
        $interval = static::positive($config, 'interval');
        $ttl = static::positive($config, 'ttl');
        $timeout = static::positive($config, 'timeout');

        if ($ttl <= $interval) {
            throw new InvalidArgumentException('Murmur metrics.ttl must be greater than metrics.interval.');
        }

        $node = $config->get('murmur.metrics.node');

        if (is_string($node) && trim($node) === '') {
            $node = null;
        }

        if ($node !== null && ! Snapshot::validNode($node)) {
            throw new InvalidArgumentException('Murmur metrics.node must be 1 to 128 characters using letters, digits, dots, underscores, colons, or hyphens.');
        }

        $only = $config->get('murmur.metrics.only');

        if (! is_array($only) || ! array_is_list($only)) {
            throw new InvalidArgumentException('Murmur metrics.only must be a list of known metric names.');
        }

        foreach ($only as $name) {
            if (! in_array($name, Metrics::names(), true)) {
                throw new InvalidArgumentException('Murmur metrics.only must be a list of known metric names.');
            }
        }

        $table = $config->get('murmur.metrics.database.table');

        if (! is_string($table) || preg_match('/\A[A-Za-z0-9_]+\z/', $table) !== 1) {
            throw new InvalidArgumentException('Murmur metrics.database.table may contain only letters, digits, and underscores.');
        }

        return new static($store, $interval, $ttl, $timeout, $node, $only, $table);
    }

    /**
     * Read a strictly positive and finite duration in seconds.
     *
     * @throws InvalidArgumentException
     */
    protected static function positive(Repository $config, string $key): float
    {
        $value = $config->get("murmur.metrics.{$key}");

        if (! is_numeric($value) || ! is_finite((float) $value) || (float) $value <= 0) {
            throw new InvalidArgumentException("Murmur metrics.{$key} must be a positive, finite number of seconds.");
        }

        return (float) $value;
    }
}
