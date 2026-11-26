<?php

namespace Arttecs\Murmur\Server;

use InvalidArgumentException;

/**
 * An event published to a topic.
 */
class Event
{
    /**
     * Create a new event.
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        public readonly string $topic,
        public readonly string $name,
        public readonly mixed $data = null,
        public readonly ?EventId $id = null,
    ) {
        if (! Topic::isValid($topic)) {
            throw new InvalidArgumentException('Invalid event topic.');
        }

        if (! self::isValidName($name)) {
            throw new InvalidArgumentException('Invalid event name.');
        }
    }

    /**
     * Get a copy of the event with the given identifier.
     */
    public function withId(EventId $id): Event
    {
        return new self($this->topic, $this->name, $this->data, $id);
    }

    /**
     * Determine if the given event name is valid.
     */
    public static function isValidName(string $name): bool
    {
        return preg_match('/\A[^\r\n]{1,200}\z/', $name) === 1;
    }

    /**
     * Determine if a publisher may send the given event name.
     *
     * Names starting with "murmur." are reserved for the server, except the stream end marker.
     */
    public static function isPublishableName(string $name): bool
    {
        return self::isValidName($name) && (! str_starts_with($name, 'murmur.') || $name === 'murmur.end');
    }
}
