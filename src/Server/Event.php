<?php

namespace Arttecs\Murmur\Server;

use InvalidArgumentException;

class Event
{
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

    public function withId(EventId $id): Event
    {
        return new self($this->topic, $this->name, $this->data, $id);
    }

    public static function isValidName(string $name): bool
    {
        return preg_match('/^[^\r\n]{1,200}\z/', $name) === 1;
    }
}
