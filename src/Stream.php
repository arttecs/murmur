<?php

namespace Arttecs\Murmur;

use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Exceptions\PublishFailed;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\Topic;

/**
 * Publish events and build a subscription URL for a single topic.
 */
class Stream
{
    /**
     * The time when the stream was created, in milliseconds.
     */
    protected int $createdAtMs;

    /**
     * Create a stream and capture its creation time.
     */
    public function __construct(
        protected MurmurManager $manager,
        protected string $topic,
        protected int $cursorMarginMs,
        protected Publisher $publisher,
    ) {
        $this->createdAtMs = now()->getTimestampMs();
    }

    /**
     * Publish an event to the stream topic.
     *
     * @throws \InvalidArgumentException
     * @throws PublishFailed
     */
    public function emit(string $event, mixed $data = null): void
    {
        $this->manager->publish($this->topic, $event, $data);
    }

    /**
     * Publish the end marker for the stream.
     *
     * @throws PublishFailed
     */
    public function close(): void
    {
        $this->publisher->publish(new Event($this->topic, 'murmur.end'));
    }

    /**
     * Build the subscription URL for the stream.
     */
    public function url(?int $ttl = null): string
    {
        $cursor = ($this->createdAtMs - $this->cursorMarginMs).'-0';
        $url = $this->manager->publicUrl().'/sse?topics='.rawurlencode($this->topic).'&lastEventId='.rawurlencode($cursor);

        if (Topic::isPrivate($this->topic)) {
            $url .= '&token='.rawurlencode($this->manager->token([$this->topic], $ttl));
        }

        return $url;
    }
}
