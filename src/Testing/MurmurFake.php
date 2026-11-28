<?php

namespace Arttecs\Murmur\Testing;

use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Contracts\PublishesToManyTopics;
use Arttecs\Murmur\Server\Event;
use PHPUnit\Framework\Assert;

/**
 * Record publications in memory and assert against them in application tests.
 */
class MurmurFake implements Publisher, PublishesToManyTopics
{
    /**
     * The events in publication order.
     *
     * @var list<Event>
     */
    protected array $events = [];

    /**
     * Record an event without sending it to the server.
     */
    public function publish(Event $event): void
    {
        $this->events[] = $event;
    }

    /**
     * Record one event for each topic in order.
     *
     * @param  list<string>  $topics
     */
    public function publishToTopics(array $topics, string $name, mixed $data = null, ?string $except = null): void
    {
        $events = array_map(fn (string $topic) => new Event($topic, $name, $data, except: $except), $topics);

        foreach ($events as $event) {
            $this->publish($event);
        }
    }

    /**
     * Assert that an event matching the topic, optional name and predicate was published.
     *
     * @param  (callable(Event): bool)|null  $callback
     */
    public function assertPublished(string $topic, ?string $event = null, ?callable $callback = null): void
    {
        $matches = $this->matching($topic, $event);

        if ($callback !== null) {
            $matches = array_filter($matches, $callback);
        }

        $description = $this->describe($topic, $event);
        $condition = $callback === null ? '' : ' matching the callback';
        Assert::assertNotEmpty($matches, "Expected {$description}{$condition} to be published.");
    }

    /**
     * Assert that the named event was published to the topic exactly this many times.
     */
    public function assertPublishedTimes(string $topic, string $event, int $times): void
    {
        $description = $this->describe($topic, $event);
        Assert::assertCount($times, $this->matching($topic, $event), "Expected {$description} to be published {$times} times.");
    }

    /**
     * Assert that the topic received no published event matching the optional name.
     */
    public function assertNotPublished(string $topic, ?string $event = null): void
    {
        $description = $this->describe($topic, $event);
        Assert::assertSame([], $this->matching($topic, $event), "Expected {$description} to remain unpublished.");
    }

    /**
     * Assert that the fake recorded no publications.
     */
    public function assertNothingPublished(): void
    {
        Assert::assertCount(0, $this->events, 'Expected no events to be published.');
    }

    /**
     * Get all recorded events in publication order.
     *
     * @return list<Event>
     */
    public function published(): array
    {
        return $this->events;
    }

    /**
     * Find recorded events on the topic with the optional name.
     *
     * @return array<int, Event>
     */
    protected function matching(string $topic, ?string $event): array
    {
        return array_filter($this->events, fn (Event $record): bool => $record->topic === $topic && ($event === null || $record->name === $event));
    }

    /**
     * Describe an event and its topic in an assertion message.
     */
    protected function describe(string $topic, ?string $event): string
    {
        return ($event === null ? 'an event' : "event [{$event}]")." on topic [{$topic}]";
    }
}
