<?php

namespace Arttecs\Murmur\Tests\Support;

use Arttecs\Murmur\Contracts\MetricsStore;
use Illuminate\Support\Facades\DB;
use RuntimeException;

class FakeMetricsStore implements MetricsStore
{
    protected int $writes = 0;

    public function __construct(protected string $file, protected string $mode) {}

    public function put(string $node, int $recordedAt, array $samples): void
    {
        $first = ++$this->writes === 1;
        $this->append(['phase' => 'start']);

        if ($this->mode === 'fail-first' && $first) {
            throw new RuntimeException('sensitive store exception');
        }

        if ($this->mode === 'query-log') {
            $connection = DB::connection('sqlite');
            if ($first) {
                $connection->whenQueryingForLongerThan(0, fn () => $this->append(['phase' => 'duration threshold']));
            }

            $duration = $connection->totalQueryDuration();
            $peak = memory_get_peak_usage();
            $allocation = str_repeat('x', 2 * 1024 * 1024);
            $raisedPeak = memory_get_peak_usage();
            unset($allocation);
            $connection->logQuery('select 1', [], 10.0);
            $connection->select('select 1');
            $this->append([
                'phase' => 'queries',
                'count' => count($connection->getQueryLog()),
                'duration' => $duration,
                'peak' => $peak,
                'raisedPeak' => $raisedPeak,
            ]);
        }

        if ($this->mode === 'noisy' && $first) {
            echo "store echo\n";
            dump('store dump');
            $chunk = str_repeat('store output ', 2048);
            for ($i = 0; $i < 32; $i++) {
                fwrite(STDOUT, $chunk);
            }
            fwrite(STDERR, "writer diagnostic line\n");
            fwrite(STDOUT, "\n");
        }

        if ($this->mode === 'stray' && $first) {
            fwrite(STDOUT, 'direct noise');
        }

        if ($this->mode === 'gate' && $first) {
            $this->append(['phase' => 'waiting']);

            while (! is_file($this->file.'.release')) {
                clearstatcache(true, $this->file.'.release');
                usleep(10_000);
            }
        }

        if ($this->mode === 'hang' && $first) {
            if (extension_loaded('pcntl')) {
                pcntl_signal(SIGTERM, SIG_IGN);
            }

            $this->append(['phase' => 'waiting']);
            $until = hrtime(true) + 15_000_000_000;

            while (hrtime(true) < $until) {
                usleep(50_000);
            }
        }

        $this->append(['phase' => 'saved', 'node' => $node, 'recorded_at' => $recordedAt, 'samples' => $samples]);
    }

    public function all(): array
    {
        return [];
    }

    public function forget(string $node): void {}

    protected function append(array $entry): void
    {
        file_put_contents($this->file, json_encode(['pid' => getmypid(), ...$entry], JSON_THROW_ON_ERROR)."\n", FILE_APPEND | LOCK_EX);
    }
}
