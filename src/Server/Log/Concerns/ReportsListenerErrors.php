<?php

namespace Arttecs\Murmur\Server\Log\Concerns;

use Throwable;

trait ReportsListenerErrors
{
    /**
     * Report an error thrown by a listener without interrupting event delivery.
     */
    protected function reportListenerError(Throwable $error): void
    {
        error_log('Murmur event listener failed: '.$error);
    }
}
