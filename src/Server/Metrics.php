<?php

namespace Arttecs\Murmur\Server;

use InvalidArgumentException;

/**
 * Collect counters and read live gauges for a single server process.
 */
class Metrics
{
    /**
     * Counter descriptions in exposition order, with their label requirements.
     */
    protected const COUNTERS = [
        'murmur_connections_opened_total' => ['SSE connections accepted.', false],
        'murmur_connections_closed_total' => ['SSE connections that ended.', false],
        'murmur_connections_rejected_total' => ['SSE requests rejected by HTTP status.', true],
        'murmur_slow_consumers_total' => ['Connections closed after exceeding the output buffer limit.', false],
        'murmur_events_received_total' => ['Events delivered by the event log to the hub.', false],
        'murmur_events_sent_total' => ['Live and replayed event frames written to connections.', false],
        'murmur_bytes_sent_total' => ['Bytes of SSE output written to connections.', false],
        'murmur_replays_total' => ['Connections that requested replay with a cursor.', false],
        'murmur_replayed_events_total' => ['Event frames written from replay.', false],
        'murmur_gaps_total' => ['Murmur gap frames written to connections.', false],
        'murmur_publish_requests_total' => ['HTTP publish responses by status.', true],
        'murmur_errors_total' => ['Errors reported, including rate-limited reports.', false],
    ];

    /**
     * The counter series indexed by metric name and canonical label set.
     *
     * @var array<string, array<string, array{labels: array<string, string>, value: int}>>
     */
    protected array $counters = [];

    /**
     * The live gauge readers indexed by metric name.
     *
     * @var array<string, array{help: string, read: callable(): (int|float)}>
     */
    protected array $gauges = [];

    /**
     * Initialize every unlabeled counter to zero.
     */
    public function __construct()
    {
        foreach (static::COUNTERS as $name => $definition) {
            if (! $definition[1]) {
                $this->increment($name, 0);
            }
        }
    }

    /**
     * Add to the counter for the given label set.
     *
     * @param  array<string, string>  $labels
     *
     * @throws InvalidArgumentException
     */
    public function increment(string $name, int $by = 1, array $labels = []): void
    {
        if (! isset(static::COUNTERS[$name])) {
            throw new InvalidArgumentException("Unknown Murmur counter [{$name}].");
        }

        $key = '';

        if ($labels !== []) {
            ksort($labels, SORT_STRING);
            $key = serialize($labels);
        }

        $this->counters[$name][$key] ??= ['labels' => $labels, 'value' => 0];
        $this->counters[$name][$key]['value'] += $by;
    }

    /**
     * Register a gauge to read when collecting a snapshot.
     *
     * @param  callable(): (int|float)  $read
     */
    public function gauge(string $name, string $help, callable $read): void
    {
        $this->gauges[$name] = compact('help', 'read');
    }

    /**
     * Read all counter series and live gauges in exposition order.
     *
     * @return list<array{name: string, type: 'counter'|'gauge', help: string, labels: array<string, string>, value: int|float}>
     */
    public function snapshot(): array
    {
        $samples = [];

        foreach (static::COUNTERS as $name => [$help]) {
            $series = $this->counters[$name] ?? [];
            ksort($series, SORT_STRING);

            foreach ($series as ['labels' => $labels, 'value' => $value]) {
                $samples[] = ['name' => $name, 'type' => 'counter', 'help' => $help, 'labels' => $labels, 'value' => $value];
            }
        }

        foreach ($this->gauges as $name => ['help' => $help, 'read' => $read]) {
            $samples[] = ['name' => $name, 'type' => 'gauge', 'help' => $help, 'labels' => [], 'value' => $read()];
        }

        return $samples;
    }

    /**
     * Read the value of one series, or null when it has no sample yet.
     *
     * @param  array<string, string>  $labels
     */
    public function value(string $name, array $labels = []): int|float|null
    {
        ksort($labels, SORT_STRING);

        foreach ($this->snapshot() as $sample) {
            if ($sample['name'] === $name && $sample['labels'] === $labels) {
                return $sample['value'];
            }
        }

        return null;
    }

    /**
     * Format a snapshot using the Prometheus text exposition format.
     */
    public function toPrometheus(): string
    {
        $lines = [];
        $declared = [];

        foreach ($this->snapshot() as ['name' => $name, 'type' => $type, 'help' => $help, 'labels' => $labels, 'value' => $value]) {
            if (! isset($declared[$name])) {
                $declared[$name] = true;
                $lines[] = '# HELP '.$name.' '.strtr($help, ['\\' => '\\\\', "\n" => '\n']);
                $lines[] = "# TYPE {$name} {$type}";
            }

            $pairs = [];

            foreach ($labels as $label => $text) {
                $pairs[] = $label.'="'.strtr($text, ['\\' => '\\\\', '"' => '\"', "\n" => '\n']).'"';
            }

            $labelSet = $pairs === [] ? '' : '{'.implode(',', $pairs).'}';
            $lines[] = "{$name}{$labelSet} {$value}";
        }

        return implode("\n", $lines)."\n";
    }
}
