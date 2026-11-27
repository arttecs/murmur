<?php

namespace Arttecs\Murmur\Exceptions;

use RuntimeException;

/**
 * Indicate that a broadcast targeted a presence channel.
 */
class PresenceNotSupported extends RuntimeException {}
