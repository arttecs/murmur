<?php

return [
    'driver' => env('MURMUR_DRIVER', 'http'),
    'secret' => env('MURMUR_SECRET'),
    'server' => [
        'host' => env('MURMUR_HOST', '127.0.0.1'),
        'port' => (int) env('MURMUR_PORT', 8080),
        'heartbeat' => 20,
        'retry_ms' => 3000,
        'log_size' => 10000,
        'max_connections' => 10000,
        'max_topics_per_connection' => 50,
        'max_payload_bytes' => 65536,
        'max_buffer_bytes' => 1048576,
        'allowed_origins' => ['*'],
    ],
    'public_url' => env('MURMUR_PUBLIC_URL', 'http://127.0.0.1:8080'),
    'publish_url' => env('MURMUR_PUBLISH_URL', 'http://127.0.0.1:8080'),
    'token_ttl' => 900,
    'stream_cursor_margin_ms' => 1000,
    'http' => ['timeout' => 2.0, 'connect_timeout' => 1.0],
    'auth' => ['path' => 'murmur/auth', 'middleware' => ['web']],
    'redis' => [
        'connection' => env('MURMUR_REDIS_CONNECTION', 'default'),
        'url' => env('MURMUR_REDIS_URL', 'redis://127.0.0.1:6379'),
        'stream' => 'murmur:events',
    ],
];
