<?php

namespace Arttecs\Murmur\Testing;

use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use PHPUnit\Framework\TestCase;
use React\EventLoop\LoopInterface;
use React\EventLoop\StreamSelectLoop;
use React\Promise\Deferred;
use React\Promise\PromiseInterface;
use Throwable;

use function React\Promise\Timer\timeout;

/**
 * Verify a custom event log against Murmur's live and replay contract.
 */
abstract class EventLogConformanceTestCase extends TestCase
{
    /**
     * The event log opened for the current test.
     */
    protected ?EventLog $log = null;

    /**
     * The isolated loop used by this test and its fixture.
     */
    protected LoopInterface $loop;

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
     * Create a loop without sharing state with the application's tests.
     */
    protected function setUp(): void
    {
        parent::setUp();

        $this->loop = new StreamSelectLoop;
    }

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

        $limited = $this->await($this->log->since($cursor, ['news'], 1));
        self::assertTrue($limited->gap);
        self::assertSame([], $limited->events);
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

    /**
     * Drive the fixture loop until a fixture promise settles.
     *
     * @template T
     *
     * @param  PromiseInterface<T>  $promise
     * @return T
     */
    protected function await(PromiseInterface $promise): mixed
    {
        $settled = false;
        $running = false;
        $value = null;
        $error = null;

        $settle = function (mixed $result, ?Throwable $reason) use (&$settled, &$running, &$value, &$error): void {
            $settled = true;
            $value = $result;
            $error = $reason;

            if ($running) {
                $this->loop->stop();
            }
        };

        timeout($promise, 3.0, $this->loop)->then(
            fn (mixed $result) => $settle($result, null),
            fn (Throwable $reason) => $settle(null, $reason),
        );

        if (! $settled) {
            $running = true;
            $this->loop->run();
            $running = false;
        }

        self::assertTrue($settled, 'The loop stopped before the conformance fixture promise settled.');

        if ($error !== null) {
            throw $error;
        }

        return $value;
    }
}
