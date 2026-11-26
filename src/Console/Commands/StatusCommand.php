<?php

namespace Arttecs\Murmur\Console\Commands;

use Arttecs\Murmur\Exceptions\MetricsUnavailable;
use Arttecs\Murmur\MurmurManager;
use Illuminate\Console\Command;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * Display metrics from a running Murmur server.
 */
class StatusCommand extends Command
{
    /**
     * The command name and its output format option.
     */
    protected $signature = 'murmur:status {--json : Print the metrics list as JSON}';

    /**
     * The command description shown by Artisan.
     */
    protected $description = 'Show metrics from the Murmur server';

    /**
     * Read and display the configured server's metrics.
     */
    public function handle(MurmurManager $murmur): int
    {
        try {
            $metrics = $murmur->metrics();
        } catch (MetricsUnavailable $error) {
            $this->error($error->getMessage());

            return self::FAILURE;
        }

        if ($this->option('json')) {
            $this->output->writeln(json_encode($metrics, JSON_THROW_ON_ERROR), OutputInterface::OUTPUT_RAW);

            return self::SUCCESS;
        }

        $rows = [];

        foreach ($metrics as ['name' => $name, 'labels' => $labels, 'value' => $value]) {
            $pairs = [];

            foreach ($labels as $label => $text) {
                $pairs[] = "{$label}={$text}";
            }

            $rows[] = [$name, implode(', ', $pairs), (string) $value];
        }

        $this->table(['Metric', 'Labels', 'Value'], $rows);

        return self::SUCCESS;
    }
}
