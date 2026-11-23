<?php

namespace Arttecs\Murmur\Server;

use Throwable;

/**
 * Rate-limit reports of errors raised during event delivery.
 */
class ErrorReporter
{
    /**
     * The monotonic time in nanoseconds when the last error was reported.
     */
    protected static int|float|null $lastReport = null;

    /**
     * The number of errors suppressed since the last report.
     */
    protected static int $suppressed = 0;

    /**
     * Report the exception class and message at most once per second.
     */
    public static function report(Throwable $error): void
    {
        $now = hrtime(true);

        if (static::$lastReport !== null && $now - static::$lastReport < 1_000_000_000) {
            static::$suppressed++;

            return;
        }

        $suffix = static::$suppressed === 0 ? '' : ' ('.static::$suppressed.' errors suppressed)';
        $message = str_replace(["\r", "\n", "\0"], ' ', $error->getMessage());
        static::$lastReport = $now;
        static::$suppressed = 0;

        error_log($error::class.': '.$message.$suffix);
    }
}
