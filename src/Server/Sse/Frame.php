<?php

namespace Arttecs\Murmur\Server\Sse;

use Arttecs\Murmur\Server\Event;

class Frame
{
    public static function event(Event $event): string
    {
        $data = json_encode(
            ['topic' => $event->topic, 'data' => $event->data],
            JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR,
        );

        $id = $event->id === null ? '' : "id: {$event->id}\n";

        return "{$id}event: {$event->name}\ndata: {$data}\n\n";
    }

    public static function comment(string $text = 'ping'): string
    {
        return ": {$text}\n\n";
    }

    public static function retry(int $ms): string
    {
        return "retry: {$ms}\n\n";
    }

    public static function gap(): string
    {
        return "event: murmur.gap\ndata: {}\n\n";
    }
}
