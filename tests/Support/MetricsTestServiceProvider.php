<?php

namespace Arttecs\Murmur\Tests\Support;

use Arttecs\Murmur\Contracts\MetricsStore;
use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\Server\Server;
use Illuminate\Support\ServiceProvider;
use React\ChildProcess\Process;
use React\EventLoop\LoopInterface;

class MetricsTestServiceProvider extends ServiceProvider
{
    public function boot(): void
    {
        Murmur::extendMetrics('fake', fn (): MetricsStore => new FakeMetricsStore(
            getenv('MURMUR_TEST_METRICS_FILE'),
            getenv('MURMUR_TEST_METRICS_MODE') ?: '',
        ));

        if (getenv('MURMUR_TEST_METRICS_DISABLED')) {
            Murmur::serving(function (Server $server, LoopInterface $loop): void {
                $loop->addTimer(0.2, function () use ($loop): void {
                    fwrite(STDOUT, class_exists(Process::class, false) ? "Recorder loaded\n" : "Recorder inactive\n");
                    $loop->stop();
                });
            });
        }
    }
}
