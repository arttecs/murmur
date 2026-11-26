<?php

use Arttecs\Murmur\Server\Http\MetricsHandler;
use Arttecs\Murmur\Server\Metrics;
use Arttecs\Murmur\Server\ServerConfig;
use React\Http\Message\ServerRequest;

it('renders exact prometheus text with escaped label values and one declaration per metric', function () {
    $metrics = new class extends Metrics
    {
        public function snapshot(): array
        {
            return [
                ['name' => 'murmur_test_total', 'type' => 'counter', 'help' => 'Test counter.', 'labels' => ['status' => '202'], 'value' => 2],
                ['name' => 'murmur_test_total', 'type' => 'counter', 'help' => 'Test counter.', 'labels' => ['status' => "line\nwith \"quotes\" and a \\ slash"], 'value' => 3],
                ['name' => 'murmur_temperature', 'type' => 'gauge', 'help' => 'Test gauge.', 'labels' => [], 'value' => 3.5],
            ];
        }
    };
    $handler = new MetricsHandler(new ServerConfig('test-secret-with-at-least-32-bytes', metricsToken: 'metrics-secret-token'), $metrics);
    $response = $handler(new ServerRequest('GET', '/metrics', ['Authorization' => 'Bearer metrics-secret-token']));
    $expected = implode("\n", [
        '# HELP murmur_test_total Test counter.',
        '# TYPE murmur_test_total counter',
        'murmur_test_total{status="202"} 2',
        'murmur_test_total{status="line\\nwith \\"quotes\\" and a \\\\ slash"} 3',
        '# HELP murmur_temperature Test gauge.',
        '# TYPE murmur_temperature gauge',
        'murmur_temperature 3.5',
        '',
    ]);

    expect($response->getStatusCode())->toBe(200);
    expect($response->getHeaderLine('Content-Type'))->toBe('text/plain; version=0.0.4; charset=utf-8');
    expect($response->getHeaderLine('Cache-Control'))->toBe('no-store');
    expect((string) $response->getBody())->toBe($expected);
});

it('returns the metrics snapshot as json when requested', function () {
    $metrics = new Metrics;
    $metrics->gauge('murmur_foo', 'Test gauge.', fn (): int => 2);
    $handler = new MetricsHandler(new ServerConfig('test-secret-with-at-least-32-bytes', metricsToken: 'metrics-secret-token'), $metrics);

    $response = $handler(new ServerRequest('GET', '/metrics', ['Accept' => 'text/plain, application/json;q=0.5', 'Authorization' => 'Bearer metrics-secret-token']));

    expect($response->getStatusCode())->toBe(200);
    expect($response->getHeaderLine('Content-Type'))->toBe('application/json');
    expect($response->getHeaderLine('Cache-Control'))->toBe('no-store');
    expect(json_decode((string) $response->getBody(), true, flags: JSON_THROW_ON_ERROR))->toBe(['metrics' => $metrics->snapshot()]);
    expect((string) $response->getBody())->toContain('"labels":{}')->not->toContain('"labels":[]');
});

it('requires the configured bearer token', function (?string $authorization, int $status) {
    $handler = new MetricsHandler(new ServerConfig('test-secret-with-at-least-32-bytes', metricsToken: 'metrics-secret-token'), new Metrics);
    $headers = $authorization === null ? [] : ['Authorization' => $authorization];

    $response = $handler(new ServerRequest('GET', '/metrics', $headers));

    expect($response->getStatusCode())->toBe($status);
    expect($response->getHeaderLine('WWW-Authenticate'))->toBe($status === 401 ? 'Bearer' : '');
})->with([
    [null, 401],
    ['Bearer wrong-secret', 401],
    ['Basic metrics-secret-token', 401],
    ['Bearer metrics-secret-token', 200],
    ['bearer metrics-secret-token', 200],
]);

it('hides the metrics route when disabled', function () {
    $handler = new MetricsHandler(new ServerConfig('test-secret-with-at-least-32-bytes', metricsEnabled: false, metricsToken: 'metrics-secret-token'), new Metrics);

    expect($handler(new ServerRequest('GET', '/metrics', ['Authorization' => 'Bearer metrics-secret-token']))->getStatusCode())->toBe(404);
});

it('hides the metrics route until a token is configured', function (array $headers) {
    $handler = new MetricsHandler(new ServerConfig('test-secret-with-at-least-32-bytes'), new Metrics);

    expect($handler(new ServerRequest('GET', '/metrics', $headers))->getStatusCode())->toBe(404);
})->with([
    'no header' => [[]],
    'empty bearer' => [['Authorization' => 'Bearer ']],
    'any bearer' => [['Authorization' => 'Bearer anything-at-all-here']],
]);

it('rejects a metrics token shorter than 16 bytes', function (string $token) {
    expect(fn () => new ServerConfig('test-secret-with-at-least-32-bytes', metricsToken: $token))
        ->toThrow(InvalidArgumentException::class, 'metricsToken');
})->with(['', 'short', str_repeat('a', 15)]);

it('accepts a 16-byte metrics token', function () {
    expect((new ServerConfig('test-secret-with-at-least-32-bytes', metricsToken: str_repeat('a', 16)))->metricsToken)->toBe(str_repeat('a', 16));
});

it('rejects an empty configured metrics token', function () {
    expect(fn () => new ServerConfig('test-secret-with-at-least-32-bytes', metricsToken: ''))
        ->toThrow(InvalidArgumentException::class, 'metricsToken');
});
