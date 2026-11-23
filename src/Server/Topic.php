<?php

namespace Arttecs\Murmur\Server;

class Topic
{
    public static function isValid(string $name): bool
    {
        return preg_match('/^[^\s,]{1,200}\z/', $name) === 1;
    }

    public static function isPrivate(string $name): bool
    {
        return str_starts_with($name, 'private-');
    }
}
