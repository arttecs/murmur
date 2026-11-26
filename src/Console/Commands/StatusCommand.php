<?php

namespace Arttecs\Murmur\Console\Commands;

use Arttecs\Murmur\Exceptions\MetricsStoreNotConfigured;
use Arttecs\Murmur\Metrics\Snapshot;
use Arttecs\Murmur\MurmurManager;
use Illuminate\Console\Command;
use InvalidArgumentException;
use Symfony\Component\Console\Output\OutputInterface;
use Throwable;

/**
 * Display recent metrics snapshots and totals from the configured store.
 *
 * @internal
 */
class StatusCommand extends Command
{
    /**
     * The command name and its display options.
     */
    protected $signature = 'murmur:status {--json : Print snapshots and totals as JSON} {--node= : Show only the specified node}';

    /**
     * The command description shown by Artisan.
     */
    protected $description = 'Show recent Murmur metrics';

    /**
     * The metric columns shown in the status table.
     */
    protected const COLUMNS = [
        'murmur_connections' => 'Open connections',
        'murmur_topics' => 'Topics',
        'murmur_events_sent_total' => 'Events sent',
        'murmur_errors_total' => 'Errors',
        'murmur_memory_bytes' => 'Memory (bytes)',
    ];

    /**
     * Read fresh snapshots and display the requested view.
     */
    public function handle(MurmurManager $manager): int
    {
        try {
            $nodes = $manager->metrics();
            $node = $this->option('node');

            if ($node !== null) {
                $nodes = array_intersect_key($nodes, [$node => true]);
            }

            if ($nodes === []) {
                $this->error('No fresh Murmur metrics snapshots found.');

                return self::FAILURE;
            }

            $totals = Snapshot::totals($nodes);

            if ($this->option('json')) {
                $this->output->writeln(json_encode(['nodes' => (object) $nodes, 'totals' => (object) $totals], JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION), OutputInterface::OUTPUT_RAW);
            } else {
                $now = now()->timestamp;
                $rows = [];

                foreach ($nodes as $name => $snapshot) {
                    $rows[] = [$name, max(0, $now - $snapshot['recorded_at']), ...$this->figures(Snapshot::totals([$snapshot]))];
                }

                $rows[] = ['Totals', '—', ...$this->figures($totals)];

                $this->table(['Node', 'Age (s)', ...array_values(static::COLUMNS)], $rows);
            }
        } catch (MetricsStoreNotConfigured|InvalidArgumentException $error) {
            $this->error($error->getMessage());

            return self::FAILURE;
        } catch (Throwable) {
            $this->error('Murmur could not read the metrics store.');

            return self::FAILURE;
        }

        return self::SUCCESS;
    }

    /**
     * Select the figures displayed in a table row.
     *
     * @param  array<string, int|float>  $totals
     * @return list<int|float|string>
     */
    protected function figures(array $totals): array
    {
        return array_map(fn (string $metric): int|float|string => $totals[$metric] ?? '—', array_keys(static::COLUMNS));
    }
}
