<?php

namespace Arttecs\Murmur\Testing;

use React\EventLoop\LoopInterface;
use React\EventLoop\StreamSelectLoop;
use React\Promise\PromiseInterface;
use Throwable;

use function React\Promise\Timer\timeout;

/**
 * Wait for conformance fixture promises on an isolated event loop.
 *
 * @internal
 */
trait AwaitsPromises
{
    /**
     * The isolated loop used by this test and its fixture.
     */
    protected LoopInterface $loop;

    /**
     * Create a loop without sharing state with the application's tests.
     */
    protected function setUp(): void
    {
        parent::setUp();

        $this->loop = new StreamSelectLoop;
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
