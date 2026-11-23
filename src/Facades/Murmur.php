<?php

namespace Arttecs\Murmur\Facades;

use Arttecs\Murmur\MurmurManager;
use Illuminate\Support\Facades\Facade;

/**
 * Access the Murmur manager through Laravel's facade.
 *
 * @method static void publish(string $topic, string $event, mixed $data = null)
 * @method static \Arttecs\Murmur\Stream stream(string $topic)
 * @method static string token(array<string> $topics, ?int $ttl = null)
 * @method static string publicUrl()
 *
 * @see MurmurManager
 */
class Murmur extends Facade
{
    /**
     * Get the container binding for the facade.
     *
     * @return class-string<MurmurManager>
     */
    protected static function getFacadeAccessor(): string
    {
        return MurmurManager::class;
    }
}
