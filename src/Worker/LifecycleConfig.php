<?php

namespace Arttecs\Murmur\Worker;

use Illuminate\Contracts\Config\Repository;
use InvalidArgumentException;

/**
 * Validate the settings for connection lifecycle delivery.
 *
 * @internal
 */
class LifecycleConfig
{
    /**
     * Hold the validated queue, batch, and timing settings.
     */
    protected function __construct(
        public readonly int $buffer,
        public readonly int $batch,
        public readonly float $interval,
        public readonly float $timeout,
    ) {}

    /**
     * Read settings for lifecycle delivery when enabled.
     *
     * @throws InvalidArgumentException
     */
    public static function from(Repository $config): ?static
    {
        $enabled = filter_var($config->get('murmur.lifecycle.enabled', false), FILTER_VALIDATE_BOOL, FILTER_NULL_ON_FAILURE);

        if ($enabled === null) {
            throw new InvalidArgumentException('Murmur lifecycle.enabled must be a boolean.');
        }

        if (! $enabled) {
            return null;
        }

        $buffer = static::positiveInteger($config, 'buffer');
        $batch = static::positiveInteger($config, 'batch');

        if ($batch > $buffer) {
            throw new InvalidArgumentException('Murmur lifecycle.batch must not exceed lifecycle.buffer.');
        }

        return new static(
            $buffer,
            $batch,
            static::positiveDuration($config->get('murmur.lifecycle.interval'), 'interval'),
            static::positiveDuration($config->get('murmur.lifecycle.timeout', 10.0), 'timeout'),
        );
    }

    /**
     * Read a strictly positive and finite duration in seconds.
     *
     * @throws InvalidArgumentException
     */
    protected static function positiveDuration(mixed $value, string $key): float
    {
        if (! is_numeric($value) || ! is_finite((float) $value) || (float) $value <= 0) {
            throw new InvalidArgumentException("Murmur lifecycle.{$key} must be a positive, finite number of seconds.");
        }

        return (float) $value;
    }

    /**
     * Read a positive integer or its environment string representation.
     *
     * @throws InvalidArgumentException
     */
    protected static function positiveInteger(Repository $config, string $key): int
    {
        $value = $config->get("murmur.lifecycle.{$key}");

        if ((! is_int($value) && ! is_string($value)) ||
            ($value = filter_var($value, FILTER_VALIDATE_INT, ['options' => ['min_range' => 1]])) === false) {
            throw new InvalidArgumentException("Murmur lifecycle.{$key} must be a positive integer.");
        }

        return $value;
    }
}
