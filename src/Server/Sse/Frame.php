<?php

namespace Arttecs\Murmur\Server\Sse;

use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Server;

/**
 * Encode server-sent event frames.
 *
 * @internal
 */
class Frame
{
    /**
     * Encode an event as a server-sent event frame.
     *
     * @throws \JsonException
     */
    public static function event(Event $event): string
    {
        $data = json_encode(
            ['topic' => $event->topic, 'data' => $event->data],
            JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR,
        );

        $id = $event->id === null ? '' : "id: {$event->id}\n";

        return "{$id}event: {$event->name}\ndata: {$data}\n\n";
    }

    /**
     * Encode a comment frame, used as a heartbeat.
     */
    public static function comment(string $text = 'ping'): string
    {
        $text = str_replace(["\r", "\n"], ' ', $text);

        return ": {$text}\n\n";
    }

    /**
     * Encode a heartbeat that advances the replay cursor.
     */
    public static function heartbeat(EventId $position): string
    {
        return "id: {$position}\nevent: murmur.heartbeat\ndata: {}\n\n";
    }

    /**
     * Encode the reconnection delay frame.
     */
    public static function retry(int $ms): string
    {
        return "retry: {$ms}\n\n";
    }

    /**
     * Announce the connection ID without setting a replay cursor.
     */
    public static function connected(string $id): string
    {
        $protocol = Server::PROTOCOL;

        return "event: murmur.connected\ndata: {\"connection_id\":\"{$id}\",\"protocol\":{$protocol}}\n\n";
    }

    /**
     * Acknowledge attachment and advance the cursor to the sampled log tail.
     */
    public static function attached(?EventId $tail): string
    {
        $id = $tail === null ? '' : "id: {$tail}\n";

        return "{$id}event: murmur.attached\ndata: {}\n\n";
    }

    /**
     * Encode the frame that signals missed events that can no longer be replayed.
     */
    public static function gap(): string
    {
        return "event: murmur.gap\ndata: {}\n\n";
    }
}
