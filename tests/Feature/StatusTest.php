<?php

use Arttecs\Murmur\Exceptions\MetricsUnavailable;
use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Illuminate\Http\Client\ConnectionException;
use Illuminate\Http\Client\Request;
use Illuminate\Support\Facades\Http;
use Symfony\Component\Process\Process;

use function React\Async\delay;

/**
 * Return a realistic response with unlabeled and labeled metrics.
 */
function statusSamples(): array
{
    return [
        ['name' => 'murmur_connections', 'type' => 'gauge', 'help' => 'Open SSE connections.', 'labels' => [], 'value' => 2],
        ['name' => 'murmur_publish_requests_total', 'type' => 'counter', 'help' => 'HTTP publish responses by status.', 'labels' => ['status' => '202'], 'value' => 5],
    ];
}

it('fetches a metrics list using the configured URL, timeouts and optional bearer token', function (?string $token) {
    config([
        'murmur.secret' => null,
        'murmur.publish_url' => 'http://murmur.test/',
        'murmur.http.timeout' => 2.5,
        'murmur.http.connect_timeout' => 0.75,
        'murmur.server.metrics.token' => $token,
    ]);
    $options = [];
    Http::fake(function (Request $request, array $requestOptions) use (&$options) {
        $options = $requestOptions;

        return Http::response(['metrics' => statusSamples()]);
    });

    expect(Murmur::metrics())->toBe(statusSamples());
    expect($options)->toMatchArray(['timeout' => 2.5, 'connect_timeout' => 0.75, 'allow_redirects' => false]);
    Http::assertSent(fn (Request $request): bool => $request->method() === 'GET'
        && $request->url() === 'http://murmur.test/metrics'
        && $request->hasHeader('Accept', 'application/json')
        && ($token === null ? ! $request->hasHeader('Authorization') : $request->hasHeader('Authorization', 'Bearer '.$token)));
})->with([null, 'scrape-secret']);

it('treats every non-200 response as unavailable without including the token', function (int $status) {
    config(['murmur.server.metrics.token' => 'scrape-secret']);
    Http::fake(['*' => Http::response('scrape-secret', $status)]);

    expect(fn () => Murmur::metrics())
        ->toThrow(MetricsUnavailable::class, "The Murmur server returned HTTP status {$status} for the metrics request.");
})->with([401, 204, 302]);

it('reports a failed metrics connection without exposing the transport message', function () {
    config(['murmur.server.metrics.token' => 'scrape-secret']);
    Http::fake(fn () => throw new ConnectionException('Connection refused with scrape-secret'));

    expect(fn () => Murmur::metrics())
        ->toThrow(MetricsUnavailable::class, 'The metrics request to the Murmur server failed.');
});

it('rejects a malformed metrics body', function (string $body) {
    Http::fake(['*' => Http::response($body)]);

    expect(fn () => Murmur::metrics())
        ->toThrow(MetricsUnavailable::class, 'The Murmur server returned malformed metrics.');
})->with([
    'not JSON' => 'no',
    'missing metrics field' => '{}',
    'object instead of list' => '{"metrics":{}}',
    'invalid sample' => '{"metrics":[{"name":"murmur_connections"}]}',
    'invalid label value' => '{"metrics":[{"name":"foo","type":"counter","help":"Help.","labels":{"status":202},"value":1}]}',
    'invalid sample value' => '{"metrics":[{"name":"foo","type":"gauge","help":"Help.","labels":[],"value":"2"}]}',
]);

it('prints metrics as a table', function () {
    Http::fake(['*' => Http::response(['metrics' => statusSamples()])]);

    $this->artisan('murmur:status')
        ->expectsTable(['Metric', 'Labels', 'Value'], [
            ['murmur_connections', '', '2'],
            ['murmur_publish_requests_total', 'status=202', '5'],
        ])
        ->assertSuccessful();
});

it('prints the raw metrics list as json', function () {
    Http::fake(['*' => Http::response(['metrics' => statusSamples()])]);

    $this->artisan('murmur:status', ['--json' => true])
        ->expectsOutput(json_encode(statusSamples(), JSON_THROW_ON_ERROR))
        ->assertSuccessful();
});

it('prints one line and exits unsuccessfully when the server is unavailable', function () {
    Http::fake(fn () => throw new ConnectionException('refused'));

    $this->artisan('murmur:status')
        ->expectsOutput('The metrics request to the Murmur server failed.')
        ->assertExitCode(1);
});

it('fetches protected metrics from a listening murmur serve process', function () {
    $port = ServerHarness::availablePort();
    $address = "http://127.0.0.1:{$port}";
    $root = dirname(__DIR__, 2);
    $token = 'test-scrape-token';
    $server = new Process([PHP_BINARY, $root.'/vendor/bin/testbench', 'murmur:serve', "--port={$port}"], $root, [
        'MURMUR_HOST' => '127.0.0.1',
        'MURMUR_SECRET' => config('murmur.secret'),
        'MURMUR_DRIVER' => 'http',
        'MURMUR_METRICS_ENABLED' => 'true',
        'MURMUR_METRICS_TOKEN' => $token,
    ]);

    try {
        $server->start();
        $deadline = hrtime(true) + 5_000_000_000;
        $ready = "Murmur is listening on {$address}";

        while ($server->isRunning() && ! str_contains($server->getOutput(), $ready) && hrtime(true) < $deadline) {
            delay(0.01);
        }

        expect($server->getOutput())->toContain($ready, message: $server->getErrorOutput());
        config(['murmur.publish_url' => $address, 'murmur.server.metrics.token' => $token]);

        $metrics = array_column(Murmur::metrics(), 'value', 'name');
        expect($metrics['murmur_connections'])->toBe(0);
        expect($metrics['murmur_uptime_seconds'])->toBeGreaterThan(0);
    } finally {
        $server->stop(0.2);
    }
});
