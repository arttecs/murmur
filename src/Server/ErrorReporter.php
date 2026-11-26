<?php

namespace Arttecs\Murmur\Server;

use Closure;
use Psr\Log\LoggerInterface;
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
     * The error counters of the listening servers.
     *
     * @var list<Closure(): void>
     */
    protected static array $counters = [];

    /**
     * The logger used by the active server.
     */
    protected static ?LoggerInterface $logger = null;

    /**
     * Set or clear the logger for delivery errors.
     */
    public static function logUsing(?LoggerInterface $logger): void
    {
        static::$logger = $logger;
    }

    /**
     * Add a hook that counts every reported error, or clear every hook.
     *
     * @param  (callable(): void)|null  $counter
     */
    public static function countUsing(?callable $counter): void
    {
        if ($counter === null) {
            static::$counters = [];
        } else {
            static::$counters[] = Closure::fromCallable($counter);
        }
    }

    /**
     * Clear the given hooks if they are still the ones installed.
     *
     * @param  (callable(): void)|null  $counter
     */
    public static function forget(?callable $counter, ?LoggerInterface $logger): void
    {
        static::$counters = array_values(array_filter(static::$counters, fn (Closure $installed): bool => $installed !== $counter));

        if ($logger !== null && static::$logger === $logger) {
            static::$logger = null;
        }
    }

    /**
     * Report the exception class and message at most once per second.
     */
    public static function report(Throwable $error): void
    {
        foreach (static::$counters as $counter) {
            try {
                $counter();
            } catch (Throwable) {
            }
        }

        $now = hrtime(true);

        if (static::$lastReport !== null && $now - static::$lastReport < 1_000_000_000) {
            static::$suppressed++;

            return;
        }

        $suffix = static::$suppressed === 0 ? '' : ' ('.static::$suppressed.' errors suppressed)';
        $message = str_replace(["\r", "\n", "\0"], ' ', $error->getMessage());
        static::$lastReport = $now;
        static::$suppressed = 0;

        $message = $error::class.': '.$message.$suffix;

        if (static::$logger !== null) {
            try {
                static::$logger->error($message, ['exception' => $error]);

                return;
            } catch (Throwable) {
            }
        }

        error_log($message);
    }
}
