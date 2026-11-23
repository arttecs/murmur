<?php

namespace Arttecs\Murmur\Exceptions;

use RuntimeException;

/**
 * Indicate that an event could not be published to the Murmur server.
 */
class PublishFailed extends RuntimeException {}
