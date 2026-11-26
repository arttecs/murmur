<?php

use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\MurmurManager;
use Arttecs\Murmur\MurmurServiceProvider;
use Arttecs\Murmur\Server\Auth\Grant;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\EventId;
use Arttecs\Murmur\Server\Metrics;
use Arttecs\Murmur\Server\Replay;
use Arttecs\Murmur\Server\Server;
use Arttecs\Murmur\Server\ServerConfig;
use Arttecs\Murmur\Stream;

it('marks every type outside the public API as internal', function () {
    $public = [
        Murmur::class,
        MurmurManager::class,
        MurmurServiceProvider::class,
        Server::class,
        ServerConfig::class,
        Metrics::class,
        Grant::class,
        Event::class,
        EventId::class,
        Replay::class,
        Stream::class,
    ];
    $directory = dirname(__DIR__, 2).'/src/';
    $files = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($directory, FilesystemIterator::SKIP_DOTS));
    $unmarked = [];

    foreach ($files as $file) {
        if ($file->getExtension() !== 'php') {
            continue;
        }

        $relative = substr($file->getPathname(), strlen($directory), -4);
        $class = 'Arttecs\\Murmur\\'.str_replace('/', '\\', $relative);
        $type = new ReflectionClass($class);
        $internal = preg_match('/@internal\b/', $type->getDocComment() ?: '') === 1;
        $listed = in_array($class, $public, true)
            || str_starts_with($relative, 'Contracts/')
            || str_starts_with($relative, 'Exceptions/')
            || (str_starts_with($relative, 'Testing/') && ! $type->isTrait());

        if ($internal === $listed) {
            $unmarked[] = $class;
        }
    }

    expect($unmarked)->toBe([]);
});

it('keeps development-only async helpers outside package source', function () {
    $directory = dirname(__DIR__, 2).'/src/';
    $files = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($directory, FilesystemIterator::SKIP_DOTS));

    foreach ($files as $file) {
        if ($file->getExtension() === 'php') {
            expect(file_get_contents($file->getPathname()))->not->toContain('React\Async');
        }
    }
});
