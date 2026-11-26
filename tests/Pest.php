<?php

use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\MurmurManager;
use Arttecs\Murmur\Server\Connection;
use Arttecs\Murmur\Server\Log\RedisEventLog;
use Arttecs\Murmur\Tests\Support\FakePublisher;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SseReader;
use Arttecs\Murmur\Tests\TestCase;
use Clue\React\Redis\Client;
use Clue\React\Redis\Factory;
use React\EventLoop\LoopInterface;
use React\EventLoop\TimerInterface;
use React\Stream\ThroughStream;
use Symfony\Component\Process\Exception\ProcessTimedOutException;
use Symfony\Component\Process\Process;

use function React\Async\await;
use function React\Async\delay;
use function React\Promise\Timer\sleep;
use function React\Promise\Timer\timeout;

pest()->extend(TestCase::class)->in('Feature/*Test.php');

pest()->in('Feature')->afterEach(function () {
    SseReader::closeAll();
    ServerHarness::stopAll();

    cleanRedisFixtures($this->redisFixtures ?? []);
});

/**
 * Delete the fixture streams and close their logs and cleanup clients.
 *
 * @param  array<string, array{log: RedisEventLog|null, client: Client}>  $fixtures
 */
function cleanRedisFixtures(array $fixtures): void
{
    $failure = null;

    foreach ($fixtures as $key => ['log' => $log, 'client' => $client]) {
        try {
            try {
                try {
                    $log?->close();
                } finally {
                    await($client->del($key));
                }
            } finally {
                $client->close();
            }
        } catch (Throwable $error) {
            $failure ??= $error;
        }
    }

    if ($failure !== null) {
        throw $failure;
    }
}

/**
 * Get the Redis URL used by the test clients.
 */
function redisTestUrl(): string
{
    return getenv('MURMUR_TEST_REDIS_URL') ?: 'redis://127.0.0.1:6379';
}

/**
 * Create a Redis stream fixture that is deleted after the test.
 *
 * @return array{Client, string, string}
 */
function redisStream(): array
{
    $url = redisTestUrl();
    $client = (new Factory)->createLazyClient($url);

    try {
        await(timeout($client->ping(), 1.0));
    } catch (Exception $error) {
        $client->close();

        if (getenv('CI') !== false) {
            throw new RuntimeException('Redis is unreachable in CI.', 0, $error);
        }

        test()->markTestSkipped('Redis is unreachable.');
    }

    $key = 'murmur:test:'.bin2hex(random_bytes(16));

    test()->target->redisFixtures[$key] = ['log' => null, 'client' => $client];

    return [$client, $key, $url];
}

/**
 * Create a Redis event log with a stream that is deleted after the test.
 *
 * @return array{RedisEventLog, string}
 */
function redisLog(int $maxLength = 10000, ?LoopInterface $loop = null, int $maxPayloadBytes = 65536): array
{
    [, $key, $url] = redisStream();
    $log = new RedisEventLog($url, $key, $maxLength, $loop, maxPayloadBytes: $maxPayloadBytes);
    test()->target->redisFixtures[$key]['log'] = $log;

    return [$log, $key];
}

/**
 * Get the Redis client's ID before the log starts blocking reads.
 */
function redisReaderId(RedisEventLog $log): int
{
    $reader = (new ReflectionProperty(RedisEventLog::class, 'reader'))->getValue($log);

    return await($reader->client('ID'));
}

/**
 * Wait until Redis is blocking on the reader's XREAD.
 */
function waitForRedisReader(Client $client, int $readerId, ?LoopInterface $loop = null): void
{
    $deadline = hrtime(true) + 3_000_000_000;

    do {
        $info = await(timeout($client->client('LIST', 'ID', $readerId), 1.0, $loop));

        if (str_contains($info, 'cmd=xread') && str_contains($info, 'flags=b')) {
            return;
        }

        await(sleep(0.01, $loop));
    } while (hrtime(true) < $deadline);

    throw new RuntimeException("Redis reader {$readerId} did not block in XREAD.");
}

/**
 * Wait for the reader to observe a disconnect and schedule its retry.
 */
function waitForRedisRetry(RedisEventLog $log, ?LoopInterface $loop = null): TimerInterface
{
    $retry = new ReflectionProperty(RedisEventLog::class, 'retry');
    $deadline = hrtime(true) + 3_000_000_000;

    do {
        $timer = $retry->getValue($log);

        if ($timer !== null) {
            return $timer;
        }

        await(sleep(0.01, $loop));
    } while (hrtime(true) < $deadline);

    throw new RuntimeException('The Redis reader did not schedule a retry.');
}

/**
 * Add an event to a test stream from a separate Redis connection.
 */
function xadd(string $key, string $topic, string $name, mixed $data = null, ?int $maxLength = null): string
{
    $client = (new Factory)->createLazyClient(redisTestUrl());
    $trim = $maxLength === null ? [] : ['MAXLEN', '=', $maxLength];

    try {
        $arguments = [...$trim, '*', 'topic', $topic, 'name', $name, 'data', json_encode($data, JSON_THROW_ON_ERROR)];

        return await($client->xadd($key, ...$arguments));
    } finally {
        $client->close();
    }
}

/**
 * Bind a publisher that records the events sent by the application.
 */
function fakePublisher(): FakePublisher
{
    $publisher = new FakePublisher;

    app()->instance(Publisher::class, $publisher);
    app()->forgetInstance(MurmurManager::class);
    Murmur::clearResolvedInstance(MurmurManager::class);

    return $publisher;
}

/**
 * Create a connection and capture the frames it writes.
 *
 * @return array{Connection, callable(): string, ThroughStream}
 */
function fakeConnection(): array
{
    $stream = new ThroughStream;
    $output = '';
    $stream->on('data', function (string $data) use (&$output): void {
        $output .= $data;
    });

    return [new Connection($stream), function () use (&$output): string {
        return $output;
    }, $stream];
}

/**
 * Send a signed publish request while the test server handles HTTP requests.
 */
function publishFromSubprocess(ServerHarness $h, string $topic, string $event, mixed $data): void
{
    $process = runPublishProcess([
        __DIR__.'/Support/publish.php',
        $h->url('/publish'),
        config('murmur.secret'),
        json_encode(compact('topic', 'event', 'data'), JSON_THROW_ON_ERROR),
    ]);

    expect($process->getExitCode())->toBe(0, $process->getOutput().$process->getErrorOutput());
}

/**
 * Run a PHP subprocess without blocking the test server's event loop.
 *
 * @param  list<string>  $arguments
 */
function runPublishProcess(array $arguments): Process
{
    $process = new Process([PHP_BINARY, ...$arguments], timeout: 5);
    try {
        $process->start();

        while ($process->isRunning()) {
            $process->checkTimeout();
            delay(0.01);
        }
    } catch (ProcessTimedOutException $error) {
        throw new RuntimeException($error->getMessage()."\n".$process->getOutput().$process->getErrorOutput(), 0, $error);
    } finally {
        $process->stop(0.1);
    }

    return $process;
}

/**
 * Create a real serving process using the metrics and lifecycle test provider.
 */
function workerServingProcess(int $port, array $env): Process
{
    return new Process([PHP_BINARY, '-d', 'error_log=', __DIR__.'/Support/metrics-artisan.php', 'murmur:serve', "--port={$port}"], env: $env + [
        'MURMUR_DRIVER' => 'http',
        'MURMUR_HOST' => '127.0.0.1',
        'MURMUR_SECRET' => config('murmur.secret'),
        'MURMUR_METRICS_STORE' => 'fake',
        'MURMUR_METRICS_INTERVAL' => '0.06',
        'MURMUR_METRICS_TIMEOUT' => '2',
        'MURMUR_METRICS_TTL' => '60',
        'MURMUR_NODE' => '',
        'MURMUR_METRICS_ONLY' => '',
    ], timeout: 5);
}

/**
 * Wait for a metrics write or subprocess state without blocking the test loop.
 */
function waitForMetricsCondition(Closure $condition, ?Process $process = null): void
{
    $deadline = hrtime(true) + 15_000_000_000;

    while (! $condition()) {
        if (hrtime(true) >= $deadline || ($process !== null && ! $process->isRunning())) {
            test()->fail('Metrics condition did not complete. '.$process?->getOutput().$process?->getErrorOutput());
        }

        delay(0.01);
    }
}

/**
 * Read the store phases written by the test writer.
 */
function metricsTrace(string $file, ?string $phase = null): array
{
    $entries = array_filter(array_map(json_decode(...), file($file, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES)));

    return array_values(array_filter($entries, fn (object $entry): bool => $phase === null || $entry->phase === $phase));
}
