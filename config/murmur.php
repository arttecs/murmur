<?php

return [
    /**
     * Transport used by Laravel and the server. Use http for one server,
     * redis to share history across servers, or a registered custom driver.
     */
    'driver' => (string) env('MURMUR_DRIVER', 'http'),

    /**
     * Shared signing secret for subscription tokens and HTTP publish requests.
     * Set the same secret of at least 32 bytes in Laravel and on every server.
     */
    'secret' => ($secret = env('MURMUR_SECRET')) === null ? null : (string) $secret,

    /**
     * Listener address, SSE timing, and connection limits. Keep the loopback
     * bind for a local proxy, and tune capacity and byte limits for your traffic.
     */
    'server' => [
        /** Address to bind. Change this to accept traffic from another host. */
        'host' => (string) env('MURMUR_HOST', '127.0.0.1'),
        /** TCP port to listen on. Change this if the default port is in use. */
        'port' => (int) env('MURMUR_PORT', 8080),
        /** Seconds between SSE heartbeats. Keep this below the proxy idle timeout. */
        'heartbeat' => (float) env('MURMUR_HEARTBEAT', 20),
        /** Milliseconds clients should wait before reconnecting. Increase this to ease reconnect spikes. */
        'retry_ms' => (int) env('MURMUR_RETRY_MS', 3000),
        /** Retained event count across all topics. Increase this to keep more replay history. */
        'log_size' => (int) env('MURMUR_LOG_SIZE', 10000),
        /** Maximum open SSE connections. Size this for your event loop and host capacity. */
        'max_connections' => (int) env('MURMUR_MAX_CONNECTIONS', 900),
        /** Maximum open TCP sockets, including SSE. Keep room above the SSE limit for HTTP requests. */
        'max_sockets' => (int) env('MURMUR_MAX_SOCKETS', 1000),
        /** Seconds allowed to complete an HTTP request. Increase this for slower uploads. */
        'request_timeout' => (float) env('MURMUR_REQUEST_TIMEOUT', 10.0),
        /** Maximum events per replay. Increase this if clients must recover longer interruptions. */
        'max_replay_events' => (int) env('MURMUR_MAX_REPLAY_EVENTS', 1000),
        /** Maximum topics in one subscription. Adjust this for clients that combine topics. */
        'max_topics_per_connection' => (int) env('MURMUR_MAX_TOPICS_PER_CONNECTION', 50),
        /** Maximum publish payload size in bytes. Increase this if events need more space. */
        'max_payload_bytes' => (int) env('MURMUR_MAX_PAYLOAD_BYTES', 65536),
        /** Maximum buffered bytes per SSE connection. Lower this to shed slow subscribers earlier. */
        'max_buffer_bytes' => (int) env('MURMUR_MAX_BUFFER_BYTES', 1048576),
        /** Comma-separated browser origins allowed for SSE. Replace * to restrict cross-origin reads. */
        'allowed_origins' => array_values(array_filter(array_map(trim(...), explode(',', (string) env('MURMUR_ALLOWED_ORIGINS', '*'))), fn (string $origin): bool => $origin !== '')),
        /** Allowed difference in seconds between the server clock and an HTTP publish timestamp. */
        'signature_tolerance' => (int) env('MURMUR_SIGNATURE_TOLERANCE', 60),
        /** Seconds to drain connections on shutdown before closing the remaining sockets. */
        'shutdown_grace' => (float) env('MURMUR_SHUTDOWN_GRACE', 5.0),
        /**
         * Laravel channel for server errors and lifecycle messages. The
         * server writes from inside the event loop. Choose a fast local
         * channel such as stderr, a file, or syslog that makes no network requests.
         * Listeners of Laravel's MessageLogged event run inside the loop too.
         */
        'log_channel' => ((string) env('MURMUR_LOG_CHANNEL', '')) ?: null,
        /**
         * Per-process metrics at GET /metrics. Set a bearer token before
         * exposing the endpoint outside a trusted network.
         */
        'metrics' => [
            /** Whether to serve the metrics endpoint. Disable this if you do not scrape the process. */
            'enabled' => filter_var(env('MURMUR_METRICS_ENABLED', true), FILTER_VALIDATE_BOOL),
            /** Bearer token required for metrics requests. Leave null to allow unauthenticated scrapes. */
            'token' => ($metricsToken = env('MURMUR_METRICS_TOKEN')) === null ? null : (string) $metricsToken,
        ],
    ],

    /**
     * Base URL for browser subscriptions. Set it to the public proxy or load
     * balancer URL when browsers reach the server through an intermediary.
     */
    'public_url' => (string) env('MURMUR_PUBLIC_URL', 'http://127.0.0.1:8080'),

    /**
     * Base URL Laravel uses for HTTP publishing and metrics. Set it to
     * the address reachable from the Laravel application.
     */
    'publish_url' => (string) env('MURMUR_PUBLISH_URL', 'http://127.0.0.1:8080'),

    /**
     * Subscription token lifetime in seconds. Adjust how long a client can
     * subscribe before it needs a fresh token.
     */
    'token_ttl' => (int) env('MURMUR_TOKEN_TTL', 900),

    /**
     * Milliseconds to subtract from a new stream cursor. Increase this if
     * clocks on the application and event hosts can differ.
     */
    'stream_cursor_margin_ms' => (int) env('MURMUR_STREAM_CURSOR_MARGIN_MS', 1000),

    /**
     * Laravel's HTTP deadlines in seconds. Adjust them for a remote server
     * or to make requests fail faster when the server is unreachable.
     */
    'http' => [
        /** Total request deadline in seconds. */
        'timeout' => (float) env('MURMUR_HTTP_TIMEOUT', 2.0),
        /** Connection deadline in seconds. */
        'connect_timeout' => (float) env('MURMUR_HTTP_CONNECT_TIMEOUT', 1.0),
    ],

    /**
     * Laravel route for exchanging private channel access for tokens.
     * Disable it if your application issues its own subscription tokens.
     */
    'auth' => [
        /** Whether to register the murmur.auth route. */
        'enabled' => filter_var(env('MURMUR_AUTH_ENABLED', true), FILTER_VALIDATE_BOOL),
        /** URL path for the authorization endpoint. Change this if the path is already in use. */
        'path' => (string) env('MURMUR_AUTH_PATH', 'murmur/auth'),
        /** Comma-separated Laravel middleware for the authorization route. Publish this file to use middleware whose parameters contain commas. */
        'middleware' => array_values(array_filter(array_map(trim(...), explode(',', (string) env('MURMUR_AUTH_MIDDLEWARE', 'web'))), fn (string $middleware): bool => $middleware !== '')),
    ],

    /**
     * Redis transport settings. Point Laravel and the server at the same
     * database and stream when using the redis driver.
     */
    'redis' => [
        /** Name of the Laravel Redis connection used to append events. */
        'connection' => (string) env('MURMUR_REDIS_CONNECTION', 'default'),
        /** Redis URL used by the server for live reading and replay. */
        'url' => (string) env('MURMUR_REDIS_URL', 'redis://127.0.0.1:6379'),
        /** Stream name before Laravel's Redis key prefix. Set this to isolate applications in one database. */
        'stream' => (string) env('MURMUR_REDIS_STREAM', 'murmur:events'),
        /** Seconds allowed for a Redis query outside the blocking live read. Increase this for slower Redis round trips. */
        'query_timeout' => (float) env('MURMUR_REDIS_QUERY_TIMEOUT', 5.0),
        /** Milliseconds Redis waits for new events during a live read. A stalled reader is replaced 5 seconds after this wait. */
        'block_ms' => (int) env('MURMUR_REDIS_BLOCK_MS', 5000),
    ],
];
