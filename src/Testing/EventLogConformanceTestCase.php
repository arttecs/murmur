<?php

namespace Arttecs\Murmur\Testing;

use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use PHPUnit\Framework\TestCase;
use React\EventLoop\LoopInterface;
use React\Promise\Deferred;
use React\Promise\PromiseInterface;
use Throwable;

/**
 * Verify a custom event log against Murmur's live and replay contract.
 */
abstract class EventLogConformanceTestCase extends TestCase
{
    use AwaitsPromises;

    /**
     * The event log opened for the current test.
     */
    protected ?EventLog $log = null;

    /**
     * Create an empty log and an append callback using the supplied ReactPHP loop.
     *
     * The callback stores an event and returns its assigned ID. Configure trimming
     * to retain at most the requested number of entries in the fixture.
     *
     * @return array{EventLog, callable(Event): (EventId|PromiseInterface<EventId>)}
     */
    abstract protected function createEventLog(int $capacity, LoopInterface $loop): array;

    /**
     * Start live delivery and verify ordered events with increasing IDs for every subscriber.
     */
    public function test_delivers_events_in_log_order_to_every_subscriber(): void
    {
        [$this->log, $append] = $this->createEventLog(10, $this->loop);
        $first = [];
        $second = [];
        $delivered = new Deferred;
        $this->log->subscribe(function (Event $event) use (&$first): void {
            $first[] = $event;
        });
        $this->log->subscribe(function (Event $event) use (&$second, $delivered): void {
            $second[] = $event;

            if (count($second) === 3) {
                $delivered->resolve(null);
            }
        });

        self::assertNull($this->await($this->log->ready()));
        self::assertTrue($this->log->healthy());
        $events = [
            new Event('news', 'first', (object) ['id' => 1]),
            new Event('weather', 'second', true),
            new Event('news', 'third'),
        ];
        $ids = array_map(fn (Event $event): EventId => $this->append($append, $event), $events);
        $this->await($delivered->promise());

        self::assertLessThan(0, $ids[0]->compare($ids[1]));
        self::assertLessThan(0, $ids[1]->compare($ids[2]));
        $expected = array_map(fn (Event $event, EventId $id): Event => $event->withId($id), $events, $ids);
        self::assertEquals($expected, $first);
        self::assertEquals($expected, $second);
    }

    /**
     * Preserve an excluded connection when storing and reading the event.
     */
    public function test_preserves_exclusions_in_live_delivery_and_replay(): void
    {
        [$this->log, $append] = $this->createEventLog(10, $this->loop);
        $cursor = $this->append($append, new Event('news', 'cursor'));
        $delivered = new Deferred;
        $this->log->subscribe($delivered->resolve(...));
        $this->await($this->log->ready());

        $except = str_repeat('a', 32);
        $id = $this->append($append, new Event('news', 'update', except: $except));
        $live = $this->await($delivered->promise());
        $replayed = $this->await($this->log->since($cursor, ['news'], 10))->events;
        $visibleId = $this->append($append, new Event('news', 'visible'));
        $filtered = $this->await($this->log->since($cursor, ['news'], 1, fn (Event $event): bool => $event->except !== $except));

        self::assertSame($except, $live->except);
        self::assertEquals([new Event('news', 'update', id: $id, except: $except)], $replayed);
        self::assertFalse($filtered->gap);
        self::assertEquals([new Event('news', 'visible', id: $visibleId)], $filtered->events);
    }

    /**
     * Replay only matching events after the cursor and enforce the matching event limit.
     */
    public function test_replays_matching_events_after_the_cursor_up_to_the_limit(): void
    {
        [$this->log, $append] = $this->createEventLog(10, $this->loop);
        $cursor = $this->append($append, new Event('news', 'first'));
        $second = new Event('news', 'second', 42);
        $secondId = $this->append($append, $second);
        $this->append($append, new Event('weather', 'other'));
        $fourth = new Event('news', 'fourth', 'payload');
        $fourthId = $this->append($append, $fourth);

        $replay = $this->await($this->log->since($cursor, ['news'], 2));
        self::assertFalse($replay->gap);
        self::assertEquals([$second->withId($secondId), $fourth->withId($fourthId)], $replay->events);
        self::assertEquals($fourthId, $replay->tail, 'The replay tail must be the newest identifier in the log.');

        $limited = $this->await($this->log->since($cursor, ['news'], 1));
        self::assertTrue($limited->gap);
        self::assertSame([], $limited->events);
    }

    /**
     * An empty topic list samples the tail without replaying events from any topic.
     */
    public function test_samples_the_tail_with_no_replay_topics(): void
    {
        [$this->log, $append] = $this->createEventLog(10, $this->loop);
        $cursor = new EventId(0, 0);
        $empty = $this->await($this->log->since($cursor, [], 10));
        self::assertNull($empty->tail);
        self::assertSame([], $empty->events);

        $id = $this->append($append, new Event('news', 'first'));
        $sample = $this->await($this->log->since($cursor, [], 10));
        self::assertEquals($id, $sample->tail);
        self::assertSame([], $sample->events);
    }

    /**
     * Hold the tail fixed when an append starts from the replay filter.
     *
     * The append can finish during the filter for a synchronous log or after the
     * query for an asynchronous one. The fixture does not control its exact
     * completion point on a remote server.
     */
    public function test_bounds_replayed_events_by_the_tail_sampled_before_filtering(): void
    {
        [$this->log, $append] = $this->createEventLog(3, $this->loop);
        $cursor = $this->append($append, new Event('news', 'cursor'));
        $this->append($append, new Event('news', 'first'));
        $tail = $this->append($append, new Event('news', 'second'));
        $pending = [];

        $replay = $this->await($this->log->since($cursor, ['news'], 10, function (Event $event) use ($append, &$pending): bool {
            if ($event->name === 'first') {
                foreach (['later-one', 'later-two', 'later-three'] as $name) {
                    $pending[] = $append(new Event('news', $name));
                }
            }

            return true;
        }));

        self::assertCount(3, $pending);

        foreach ($pending as $result) {
            $id = $result instanceof PromiseInterface ? $this->await($result) : $result;
            self::assertGreaterThan(0, $id->compare($tail));
        }

        self::assertEquals($tail, $replay->tail);

        foreach ($replay->events as $event) {
            self::assertNotNull($event->id);
            self::assertLessThanOrEqual(0, $event->id->compare($replay->tail));
        }

        if (! $replay->gap) {
            self::assertSame(['first', 'second'], array_column($replay->events, 'name'));
        }
    }

    /**
     * Report a gap when replay cannot cover the cursor because entries were trimmed.
     */
    public function test_reports_a_gap_when_history_after_the_cursor_was_trimmed(): void
    {
        [$this->log, $append] = $this->createEventLog(2, $this->loop);
        $cursor = $this->append($append, new Event('news', 'first'));
        $this->append($append, new Event('news', 'second'));
        $this->append($append, new Event('news', 'third'));
        $this->append($append, new Event('news', 'fourth'));

        self::assertTrue($this->await($this->log->since($cursor, ['news'], 10))->gap);
    }

    /**
     * Replay history that is still retained after older entries were trimmed, without a gap.
     */
    public function test_replays_retained_history_across_topics_after_trimming(): void
    {
        [$this->log, $append] = $this->createEventLog(3, $this->loop);
        $this->append($append, new Event('news', 'first'));
        $cursor = $this->append($append, new Event('news', 'second'));
        $third = new Event('weather', 'third');
        $thirdId = $this->append($append, $third);
        $fourth = new Event('news', 'fourth');
        $fourthId = $this->append($append, $fourth);

        $replay = $this->await($this->log->since($cursor, ['news', 'weather'], 10));

        self::assertFalse($replay->gap, 'A cursor inside the retained history must not report a gap.');
        self::assertEquals([$third->withId($thirdId), $fourth->withId($fourthId)], $replay->events);
        self::assertEquals($fourthId, $replay->tail);
    }

    /**
     * Deliver only events appended after a listener subscribed, and nothing after the log closes.
     */
    public function test_delivers_only_new_events_and_nothing_after_close(): void
    {
        [$this->log, $append] = $this->createEventLog(10, $this->loop);
        $early = [];
        $late = [];
        $delivered = new Deferred;
        $this->log->subscribe(function (Event $event) use (&$early): void {
            $early[] = $event->name;
        });
        $this->await($this->log->ready());
        $this->append($append, new Event('news', 'before'));
        $this->log->subscribe(function (Event $event) use (&$late, $delivered): void {
            $late[] = $event->name;
            $delivered->resolve(null);
        });
        $this->append($append, new Event('news', 'after'));
        $this->await($delivered->promise());

        self::assertSame(['before', 'after'], $early);
        self::assertSame(['after'], $late, 'A listener must not receive events appended before it subscribed.');

        $this->log->close();

        try {
            $this->append($append, new Event('news', 'closed'));
        } catch (Throwable) {
        }

        $settled = new Deferred;
        $this->loop->addTimer(0.05, fn () => $settled->resolve(null));
        $this->await($settled->promise());

        self::assertSame(['before', 'after'], $early, 'A closed log must not deliver events.');
    }

    /**
     * Verify that close is idempotent and the log can no longer become ready.
     */
    public function test_closing_a_log_makes_it_unhealthy_and_unavailable(): void
    {
        [$this->log] = $this->createEventLog(10, $this->loop);
        $this->log->subscribe(fn (Event $event) => null);
        $this->await($this->log->ready());

        $this->log->close();
        $this->log->close();

        self::assertFalse($this->log->healthy());
        $rejected = $this->await($this->log->ready()->then(fn () => false, fn () => true));
        self::assertTrue($rejected, 'The closed event log must reject its readiness promise.');
    }

    /**
     * Close the fixture even when a test fails.
     */
    protected function tearDown(): void
    {
        try {
            $this->log?->close();
        } finally {
            parent::tearDown();
        }
    }

    /**
     * Write one fixture event whether the append callback is synchronous or asynchronous.
     *
     * @param  callable(Event): (EventId|PromiseInterface<EventId>)  $append
     */
    protected function append(callable $append, Event $event): EventId
    {
        $id = $append($event);

        return $id instanceof PromiseInterface ? $this->await($id) : $id;
    }
}
