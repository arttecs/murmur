<?php

namespace Arttecs\Murmur\Tests\Support;

use Psr\Http\Message\ResponseInterface;
use React\EventLoop\Loop;
use React\Http\Browser;
use React\Promise\Deferred;
use React\Stream\ReadableStreamInterface;
use RuntimeException;

use function React\Async\await;

/**
 * Read server-sent event frames from an HTTP response.
 */
class SseReader
{
    /**
     * The readers to close after each test.
     *
     * @var list<SseReader>
     */
    private static array $readers = [];

    /**
     * The incomplete frame at the end of the last chunk.
     */
    private string $buffer = '';

    /**
     * The complete frames waiting to be read.
     *
     * @var list<string>
     */
    private array $frames = [];

    /**
     * The pending wait for a complete frame.
     *
     * @var Deferred<null>|null
     */
    private ?Deferred $pending = null;

    /**
     * Start listening to the response body before the first data arrives.
     */
    private function __construct(private ResponseInterface $response)
    {
        $body = $response->getBody();
        assert($body instanceof ReadableStreamInterface);

        $body->on('data', function (string $chunk): void {
            $frames = explode("\n\n", $this->buffer.$chunk);
            $this->buffer = array_pop($frames);
            array_push($this->frames, ...$frames);

            if ($this->frames !== []) {
                $this->pending?->resolve(null);
            }
        });

        $body->on('close', function (): void {
            $this->pending?->reject(new RuntimeException('The SSE stream closed before a frame arrived.'));
        });
    }

    /**
     * Open a streaming HTTP response without rejecting error statuses.
     *
     * @param  array<string, string>  $headers
     */
    public static function open(string $url, array $headers = []): SseReader
    {
        $response = (new Browser)->withRejectErrorResponse(false)->requestStreaming('GET', $url, $headers);

        return self::$readers[] = await($response->then(fn (ResponseInterface $response): self => new self($response)));
    }

    /**
     * Get the HTTP status.
     */
    public function status(): int
    {
        return $this->response->getStatusCode();
    }

    /**
     * Get the first value of a response header.
     */
    public function header(string $name): ?string
    {
        return $this->response->getHeader($name)[0] ?? null;
    }

    /**
     * Wait for and parse the next complete frame.
     *
     * @return array{id: ?string, event: ?string, data: ?string, retry: ?string, comment: ?string}
     *
     * @throws RuntimeException
     */
    public function next(float $timeout = 1.0): array
    {
        if ($this->frames === []) {
            if (! $this->response->getBody()->isReadable()) {
                throw new RuntimeException('The SSE stream closed before a frame arrived.');
            }

            $this->pending = $pending = new Deferred;
            $timer = Loop::addTimer($timeout, fn () => $pending->reject(new RuntimeException('Timed out waiting for an SSE frame.')));

            try {
                await($pending->promise());
            } finally {
                Loop::cancelTimer($timer);
                $this->pending = null;
            }
        }

        $frame = ['id' => null, 'event' => null, 'data' => null, 'retry' => null, 'comment' => null];

        foreach (explode("\n", array_shift($this->frames)) as $line) {
            [$field, $value] = explode(':', $line, 2) + ['', ''];
            $field = $field === '' ? 'comment' : $field;

            if (array_key_exists($field, $frame)) {
                $frame[$field] = str_starts_with($value, ' ') ? substr($value, 1) : $value;
            }
        }

        return $frame;
    }

    /**
     * Close the response stream.
     */
    public function close(): void
    {
        $this->response->getBody()->close();
    }

    /**
     * Close every reader opened during the test.
     */
    public static function closeAll(): void
    {
        foreach (self::$readers as $reader) {
            $reader->close();
        }

        self::$readers = [];
    }
}
