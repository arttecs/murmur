<?php

[, $url, $secret, $body] = $argv;
$timestamp = time();
$signature = hash_hmac('sha256', "{$timestamp}.{$body}", $secret);
$context = stream_context_create(['http' => [
    'method' => 'POST',
    'header' => [
        'Content-Type: application/json',
        "X-Murmur-Timestamp: {$timestamp}",
        "X-Murmur-Signature: {$signature}",
    ],
    'content' => $body,
    'ignore_errors' => true,
    'timeout' => 2,
]]);

$response = fopen($url, 'r', false, $context);

if ($response === false) {
    fwrite(STDERR, "Could not reach {$url}.\n");

    exit(1);
}

$status = stream_get_meta_data($response)['wrapper_data'][0] ?? '';
fclose($response);

if (! str_contains($status, ' 202 ')) {
    fwrite(STDERR, "Unexpected publish response: {$status}\n");
    exit(1);
}
