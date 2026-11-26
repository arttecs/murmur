<?php

namespace Arttecs\Murmur\Tests\Support;

use Arttecs\Murmur\Contracts\MetricsStore;
use Arttecs\Murmur\Events\ConnectionClosed;
use Arttecs\Murmur\Events\ConnectionOpened;
use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\Server\Server;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\ServiceProvider;
use Psr\Log\AbstractLogger;
use React\ChildProcess\Process;
use React\EventLoop\LoopInterface;
use RuntimeException;
use Stringable;

class MetricsTestServiceProvider extends ServiceProvider
{
    public function boot(): void
    {
        if ($lifecycleFile = getenv('MURMUR_TEST_LIFECYCLE_FILE')) {
            $this->traceLifecycle($lifecycleFile, getenv('MURMUR_TEST_LIFECYCLE_MODE') ?: '');
        }

        Murmur::extendMetrics('fake', function (): MetricsStore {
            $mode = getenv('MURMUR_TEST_METRICS_MODE') ?: '';

            if ($mode === 'query-log') {
                config(['database.connections.sqlite' => ['driver' => 'sqlite', 'database' => ':memory:']]);
                $connection = DB::connection('sqlite');
                $connection->enableQueryLog();
                $connection->select('select 1');
            }

            return new FakeMetricsStore(getenv('MURMUR_TEST_METRICS_FILE'), $mode);
        });

        if (getenv('MURMUR_TEST_METRICS_MODE') === 'fail-first') {
            $this->app->make(ExceptionHandler::class)->reportable(function (RuntimeException $error): void {
                file_put_contents(getenv('MURMUR_TEST_METRICS_FILE'), json_encode([
                    'pid' => getmypid(), 'phase' => 'reported', 'message' => $error->getMessage(),
                ], JSON_THROW_ON_ERROR)."\n", FILE_APPEND | LOCK_EX);

                throw new RuntimeException('The test exception handler failed.');
            });
        }

        if ($file = getenv('MURMUR_TEST_METRICS_SOCKET_TEST')) {
            Murmur::serving(function (Server $server, LoopInterface $loop) use ($file): void {
                $loop->addPeriodicTimer(0.01, function () use ($server, $file): void {
                    if (is_file($file.'.release')) {
                        $server->close();
                    }
                });
            });
        }

        if ($file = getenv('MURMUR_TEST_METRICS_SHUTDOWN_TEST')) {
            config(['murmur.server.log_channel' => 'metrics-shutdown', 'logging.channels.metrics-shutdown.driver' => 'metrics-shutdown']);
            Log::extend('metrics-shutdown', fn () => new class($file) extends AbstractLogger
            {
                public function __construct(protected string $file) {}

                public function log($level, string|Stringable $message, array $context = []): void
                {
                    if ($message === 'Murmur graceful shutdown finished.') {
                        touch($this->file.'.release');
                    }
                }
            });
        }

        if (getenv('MURMUR_TEST_METRICS_DISABLED')) {
            Murmur::serving(function (Server $server, LoopInterface $loop): void {
                $loop->addTimer(0.2, function () use ($loop): void {
                    fwrite(STDOUT, class_exists(Process::class, false) ? "Recorder loaded\n" : "Recorder inactive\n");
                    $loop->stop();
                });
            });
        }
    }

    protected function traceLifecycle(string $file, string $mode): void
    {
        $record = static function (array $fields) use ($file): void {
            file_put_contents($file, json_encode(['pid' => getmypid(), ...$fields], JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION)."\n", FILE_APPEND | LOCK_EX);
        };

        $this->app['events']->listen([ConnectionOpened::class, ConnectionClosed::class], function (ConnectionOpened|ConnectionClosed $event) use ($record, $mode): void {
            $fields = (array) $event;
            $fields['topics'] = array_map(base64_encode(...), $event->topics);
            $record(['phase' => $event instanceof ConnectionOpened ? 'opened' : 'closed', ...$fields]);

            if ($mode === 'fail-first' && $event instanceof ConnectionOpened) {
                throw new RuntimeException('The lifecycle listener failed.');
            }
        });

        if ($mode === 'fail-first') {
            $this->app->make(ExceptionHandler::class)->reportable(function (RuntimeException $error) use ($record): void {
                $record(['phase' => 'reported', 'message' => $error->getMessage()]);

                throw new RuntimeException('The test exception handler failed.');
            });
        }

        if (in_array($mode, ['error', 'slow_consumer'], true)) {
            Murmur::extend('lifecycle-test', fn () => new FakePublisher, fn () => new LifecycleReplayLog($mode));
            config(['murmur.driver' => 'lifecycle-test']);
        }
    }
}
