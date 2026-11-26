<?php

namespace Arttecs\Murmur\Console\Commands;

use Arttecs\Murmur\Exceptions\MetricsStoreNotConfigured;
use Arttecs\Murmur\Metrics\Snapshot;
use Arttecs\Murmur\MurmurManager;
use Illuminate\Console\Command;
use InvalidArgumentException;
use Throwable;

/**
 * Remove a node or the stale snapshots in the configured store.
 */
class ForgetMetricsCommand extends Command
{
    /**
     * The command name and its optional node argument.
     */
    protected $signature = 'murmur:forget-metrics {node? : The node to remove}';

    /**
     * The command description shown by Artisan.
     */
    protected $description = 'Remove a Murmur metrics snapshot or clear stale snapshots';

    /**
     * Forget the requested node or all nodes at least as old as the TTL.
     */
    public function handle(MurmurManager $manager): int
    {
        try {
            $config = $manager->metricsConfiguration() ?? throw new MetricsStoreNotConfigured;
            $store = $manager->metricsStore();
            $node = $this->argument('node');

            if ($node !== null) {
                if (! Snapshot::validNode($node)) {
                    $this->error('Invalid Murmur metrics node name.');

                    return self::FAILURE;
                }

                $store->forget($node);

                return self::SUCCESS;
            }

            $cutoff = now()->timestamp - $config->ttl;

            foreach ($store->all() as $name => $snapshot) {
                if ($snapshot['recorded_at'] <= $cutoff) {
                    $store->forget((string) $name);
                }
            }
        } catch (MetricsStoreNotConfigured|InvalidArgumentException $error) {
            $this->error($error->getMessage());

            return self::FAILURE;
        } catch (Throwable) {
            $this->error('Murmur could not remove the metrics snapshots.');

            return self::FAILURE;
        }

        return self::SUCCESS;
    }
}
