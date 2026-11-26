<?php

namespace Arttecs\Murmur\Console\Commands;

use Arttecs\Murmur\Metrics\Snapshot;
use Arttecs\Murmur\MurmurManager;
use Arttecs\Murmur\Server\Metrics;
use Illuminate\Console\Command;
use InvalidArgumentException;
use stdClass;
use Symfony\Component\Console\Output\OutputInterface;
use Throwable;

/**
 * Receive and store a single server snapshot from standard input.
 */
class RecordMetricsCommand extends Command
{
    /**
     * Maximum size of the snapshot document in bytes.
     */
    protected const MAX_BYTES = 1_048_576;

    /**
     * The command name used by the server's recorder.
     */
    protected $signature = 'murmur:record-metrics';

    /**
     * The description of the internal command.
     */
    protected $description = 'Record a Murmur metrics snapshot from standard input';

    /**
     * Hide the writer from the Artisan command list.
     */
    protected $hidden = true;

    /**
     * Validate one snapshot and hand it to the configured store.
     */
    public function handle(MurmurManager $manager): int
    {
        $json = @stream_get_contents(STDIN, static::MAX_BYTES + 1);
        $data = is_string($json) && strlen($json) <= static::MAX_BYTES ? json_decode($json) : null;

        if (! $data instanceof stdClass || count((array) $data) !== 3 || ! Snapshot::validNode($data->node ?? null)) {
            return $this->failure('Invalid Murmur metrics snapshot.');
        }

        $snapshot = Snapshot::fromDecoded($data->recorded_at ?? null, $data->samples ?? null);

        if ($snapshot === null || array_diff(array_column($snapshot['samples'], 'name'), Metrics::names()) !== []) {
            return $this->failure('Invalid Murmur metrics snapshot.');
        }

        try {
            $config = $manager->metricsConfiguration();
        } catch (InvalidArgumentException $error) {
            return $this->failure($error->getMessage());
        }

        if ($config === null) {
            return $this->failure('No Murmur metrics store is configured.');
        }

        try {
            $manager->metricsStore()->put($data->node, $snapshot['recorded_at'], $snapshot['samples']);
        } catch (Throwable $error) {
            try {
                report($error);
            } catch (Throwable) {
            }

            return $this->failure('Murmur could not write the metrics snapshot.');
        }

        return self::SUCCESS;
    }

    /**
     * Report a single error line to standard error.
     */
    protected function failure(string $message): int
    {
        $this->output->getErrorStyle()->writeln($message, OutputInterface::OUTPUT_RAW);

        return self::FAILURE;
    }
}
