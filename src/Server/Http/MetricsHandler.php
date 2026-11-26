<?php

namespace Arttecs\Murmur\Server\Http;

use Arttecs\Murmur\Server\Metrics;
use Arttecs\Murmur\Server\ServerConfig;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use React\Http\Message\Response;

/**
 * Expose a server's metrics as Prometheus text or JSON.
 */
class MetricsHandler
{
    /**
     * The registry to read when metrics are requested.
     */
    protected Metrics $metrics;

    /**
     * Create a metrics endpoint with the server's access settings.
     */
    public function __construct(protected ServerConfig $config, ?Metrics $metrics = null)
    {
        $this->metrics = $metrics ?? new Metrics;
    }

    /**
     * Authorize the scrape and return the requested metrics format.
     */
    public function __invoke(ServerRequestInterface $request): ResponseInterface
    {
        $token = $this->config->metricsToken;

        if (! $this->config->metricsEnabled || $token === null) {
            return new Response(404);
        }

        $authorization = $request->getHeaderLine('Authorization');

        if (strncasecmp($authorization, 'Bearer ', 7) !== 0 || ! hash_equals($token, substr($authorization, 7))) {
            return new Response(401, ['WWW-Authenticate' => 'Bearer']);
        }

        $metrics = $this->metrics->snapshot();
        $headers = ['Cache-Control' => 'no-store'];

        if (stripos($request->getHeaderLine('Accept'), 'application/json') !== false) {
            $samples = array_map(fn (array $sample): array => array_replace($sample, ['labels' => (object) $sample['labels']]), $metrics);

            return new Response(200, $headers + ['Content-Type' => 'application/json'], json_encode(['metrics' => $samples], JSON_THROW_ON_ERROR));
        }

        return new Response(200, $headers + ['Content-Type' => 'text/plain; version=0.0.4; charset=utf-8'], $this->prometheus($metrics));
    }

    /**
     * Format the samples using Prometheus text exposition.
     *
     * @param  list<array{name: string, type: 'counter'|'gauge', help: string, labels: array<string, string>, value: int|float}>  $metrics
     */
    protected function prometheus(array $metrics): string
    {
        $lines = [];
        $declared = [];

        foreach ($metrics as ['name' => $name, 'type' => $type, 'help' => $help, 'labels' => $labels, 'value' => $value]) {
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
