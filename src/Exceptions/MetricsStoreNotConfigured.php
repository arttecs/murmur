<?php

namespace Arttecs\Murmur\Exceptions;

use LogicException;

/**
 * Indicate that Murmur metrics persistence is disabled.
 */
class MetricsStoreNotConfigured extends LogicException
{
    /**
     * Create the exception for a missing store.
     */
    public function __construct()
    {
        parent::__construct('No Murmur metrics store is configured.');
    }
}
