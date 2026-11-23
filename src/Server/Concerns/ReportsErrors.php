<?php

namespace Arttecs\Murmur\Server\Concerns;

use Throwable;

/**
 * Report errors raised during event delivery.
 */
trait ReportsErrors
{
    /**
     * Report an error without interrupting event delivery.
     */
    protected function reportError(Throwable $error): void
    {
        error_log('Murmur event delivery failed: '.$error);
    }
}
