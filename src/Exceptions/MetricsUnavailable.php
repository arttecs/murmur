<?php

namespace Arttecs\Murmur\Exceptions;

use RuntimeException;

/**
 * Indicate that metrics could not be read from the Murmur server.
 */
class MetricsUnavailable extends RuntimeException {}
