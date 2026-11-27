<?php

use Arttecs\Murmur\Server\ErrorReporter;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Server\Server;
use Arttecs\Murmur\Server\ServerConfig;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SseReader;
use Psr\Log\AbstractLogger;
use React\EventLoop\StreamSelectLoop;

use function React\Async\await;

beforeEach(function () {
    $this->logger = new class extends AbstractLogger
    {
        public array $entries = [];

        public bool $throws = false;

        public function log($level, string|Stringable $message, array $context = []): void
        {
            if ($this->throws) {
                throw new RuntimeException('Logger failed.');
            }

            $this->entries[] = [$level, $message, $context];
        }
    };
    $this->lastReport = new ReflectionProperty(ErrorReporter::class, 'lastReport');
    $this->suppressed = new ReflectionProperty(ErrorReporter::class, 'suppressed');
    $this->lastReport->setValue(null, null);
    $this->suppressed->setValue(null, 0);
    $this->errorLog = tempnam(sys_get_temp_dir(), 'murmur');
    $this->previousErrorLog = ini_set('error_log', $this->errorLog);
});

afterEach(function () {
    ErrorReporter::logUsing(null);
    ErrorReporter::countUsing(null);
    $this->lastReport->setValue(null, null);
    $this->suppressed->setValue(null, 0);
    ini_set('error_log', (string) $this->previousErrorLog);
    unlink($this->errorLog);
});

it('sends errors to the logger with the exception context', function () {
    ini_set('error_log', $this->errorLog);
    ErrorReporter::logUsing($this->logger);
    $error = new RuntimeException("Delivery\nfailed.");

    ErrorReporter::report($error);

    expect($this->logger->entries)->toBe([['error', 'RuntimeException: Delivery failed.', ['exception' => $error]]]);
    expect(file_get_contents($this->errorLog))->toBe('');
});

it('falls back to the PHP error log and continues delivery when the logger throws', function () {
    ini_set('error_log', $this->errorLog);
    $this->logger->throws = true;
    ErrorReporter::logUsing($this->logger);
    $log = new MemoryEventLog;
    $delivered = [];
    $log->subscribe(fn () => throw new RuntimeException('The first listener failed.'));
    $log->subscribe(function (Event $event) use (&$delivered): void {
        $delivered[] = $event->name;
    });

    $log->append(new Event('news', 'updated'));

    expect($delivered)->toBe(['updated']);
    expect(file_get_contents($this->errorLog))->toContain('RuntimeException: The first listener failed.');
});

it('rate-limits logger calls and includes the number of suppressed errors in the next report', function () {
    ini_set('error_log', $this->errorLog);
    ErrorReporter::logUsing($this->logger);
    $first = new RuntimeException('First.');
    $last = new LogicException('Last.');

    ErrorReporter::report($first);
    ErrorReporter::report(new RuntimeException('Suppressed.'));
    ErrorReporter::report(new RuntimeException('Also suppressed.'));
    expect($this->logger->entries)->toBe([['error', 'RuntimeException: First.', ['exception' => $first]]]);

    $this->lastReport->setValue(null, hrtime(true) - 1_000_000_000);
    ErrorReporter::report($last);

    expect($this->logger->entries)->toBe([
        ['error', 'RuntimeException: First.', ['exception' => $first]],
        ['error', 'LogicException: Last. (2 errors suppressed)', ['exception' => $last]],
    ]);
});

it('uses the server logger while listening and reports graceful shutdown transitions', function () {
    ini_set('error_log', $this->errorLog);
    $port = ServerHarness::availablePort();
    $config = new ServerConfig('test-secret-with-at-least-32-bytes', port: $port);
    $server = new Server($config, new MemoryEventLog, logger: $this->logger);
    $error = new RuntimeException('While listening.');

    try {
        await($server->listen());
        ErrorReporter::report($error);
        await($server->shutdown());
    } finally {
        $server->close();
    }

    $this->lastReport->setValue(null, null);
    ErrorReporter::report(new RuntimeException('After close.'));

    expect($this->logger->entries)->toBe([
        ['info', "Murmur is listening on http://127.0.0.1:{$port}", []],
        ['error', 'RuntimeException: While listening.', ['exception' => $error]],
        ['info', 'Murmur graceful shutdown started.', []],
        ['info', 'Murmur graceful shutdown finished.', []],
    ]);
    expect(file_get_contents($this->errorLog))->toContain('RuntimeException: After close.');
});

it('still listens and drains when lifecycle logging fails', function () {
    ini_set('error_log', $this->errorLog);
    $this->logger->throws = true;
    $port = ServerHarness::availablePort();
    $config = new ServerConfig('test-secret-with-at-least-32-bytes', port: $port);
    $server = new Server($config, new MemoryEventLog, logger: $this->logger);

    try {
        await($server->listen());
        expect($server->address())->toBe("http://127.0.0.1:{$port}");
        await($server->shutdown());
    } finally {
        $server->close();
    }

    expect(file_get_contents($this->errorLog))->toContain('Murmur graceful shutdown finished.');
});

it('keeps the hooks of a server that is still listening when another one closes', function () {
    ini_set('error_log', $this->errorLog);
    $first = new Server(new ServerConfig('test-secret-with-at-least-32-bytes', port: ServerHarness::availablePort()), new MemoryEventLog, logger: $this->logger);
    $second = new Server(new ServerConfig('test-secret-with-at-least-32-bytes', port: ServerHarness::availablePort()), new MemoryEventLog);
    $error = new RuntimeException('Still owned.');

    try {
        await($first->listen());
        await($second->listen());
        $second->close();
        ErrorReporter::report($error);

        expect($this->logger->entries)->toContain(['error', 'RuntimeException: Still owned.', ['exception' => $error]]);
        expect(collect($first->metrics()->snapshot())->firstWhere('name', 'murmur_errors_total')['value'])->toBe(1);
    } finally {
        $first->close();
        $second->close();
    }
});

it('does not let a throwing error counter interrupt a report', function () {
    ini_set('error_log', $this->errorLog);
    ErrorReporter::logUsing($this->logger);
    ErrorReporter::countUsing(fn () => throw new RuntimeException('Counter failed.'));

    ErrorReporter::report(new RuntimeException('Reported anyway.'));

    expect(array_column($this->logger->entries, 1))->toBe(['RuntimeException: Reported anyway.']);
});

it('reports and counts a release failure when replay cannot be encoded', function () {
    ini_set('error_log', $this->errorLog);
    $log = new MemoryEventLog;
    $cursor = $log->append(new Event('news', 'seen'))->id;
    $log->append(new Event('news', 'unencodable', INF));
    $h = ServerHarness::start(log: $log);
    ErrorReporter::logUsing($this->logger);

    $r = SseReader::open($h->url('/sse?topics=news'), ['Last-Event-ID' => (string) $cursor]);
    $r->start(replaying: true);
    expect(fn () => $r->nextRaw())->toThrow(RuntimeException::class, 'The SSE stream closed');

    expect($h->metrics()->value('murmur_replay_failures_total'))->toBe(1);
    expect($h->metrics()->value('murmur_errors_total'))->toBe(1);
    expect($this->logger->entries)->toHaveCount(1);
    expect($this->logger->entries[0][2]['exception'])->toBeInstanceOf(JsonException::class);
});

it('measures blocked loop ticks and warns once while counting each breach', function () {
    ini_set('error_log', $this->errorLog);
    $loop = new StreamSelectLoop;
    $server = new Server(
        new ServerConfig('test-secret-with-at-least-32-bytes', port: ServerHarness::availablePort(), loopLagThresholdMs: 80),
        new MemoryEventLog,
        $loop,
        $this->logger,
    );

    try {
        await($server->listen());
        $loop->addTimer(0.25, fn () => usleep(180_000));
        $loop->addTimer(0.75, fn () => usleep(180_000));
        $loop->addTimer(1.9, $loop->stop(...));
        $loop->run();

        expect($server->metrics()->value('murmur_event_loop_lag_seconds'))->toBeFloat()->toBeGreaterThan(0.01);
        expect($server->metrics()->value('murmur_event_loop_lag_exceeded_total'))->toBe(2);
        $warnings = array_values(array_filter($this->logger->entries, fn (array $entry) => $entry[0] === 'warning'));
        expect($warnings)->toHaveCount(1);
        expect((string) $warnings[0][1])->toMatch('/\d+\.\d+ ms/');
    } finally {
        $server->close();
    }
});

it('does not warn below the threshold and cancels lag measurement on close', function () {
    ini_set('error_log', $this->errorLog);
    $loop = new StreamSelectLoop;
    $server = new Server(
        new ServerConfig('test-secret-with-at-least-32-bytes', port: ServerHarness::availablePort(), loopLagThresholdMs: 1000),
        new MemoryEventLog,
        $loop,
        $this->logger,
    );

    try {
        await($server->listen());
        $loop->addTimer(1.05, $loop->stop(...));
        $loop->run();
        $lag = $server->metrics()->value('murmur_event_loop_lag_seconds');
        expect($lag)->toBeFloat()->toBeGreaterThanOrEqual(0.0);
        expect($server->metrics()->value('murmur_event_loop_lag_exceeded_total'))->toBe(0);
        expect(array_column($this->logger->entries, 0))->toBe(['info']);

        $server->close();
        $loop->addTimer(0.9, fn () => usleep(180_000));
        $loop->addTimer(1.2, $loop->stop(...));
        $loop->run();
        expect($server->metrics()->value('murmur_event_loop_lag_seconds'))->toBe($lag);
    } finally {
        $server->close();
    }
});

it('reports a blocked loop through the PHP error log without a logger', function () {
    ini_set('error_log', $this->errorLog);
    $loop = new StreamSelectLoop;
    $server = new Server(
        new ServerConfig('test-secret-with-at-least-32-bytes', port: ServerHarness::availablePort(), loopLagThresholdMs: 80),
        new MemoryEventLog,
        $loop,
    );

    try {
        await($server->listen());
        $loop->addTimer(0.9, fn () => usleep(180_000));
        $loop->addTimer(1.2, $loop->stop(...));
        $loop->run();

        expect(file_get_contents($this->errorLog))->toMatch('/event loop lag.*\d+\.\d+ ms/');
    } finally {
        $server->close();
    }
});
