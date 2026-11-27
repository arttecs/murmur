<?php

namespace Arttecs\Murmur\Console\Commands;

use Arttecs\Murmur\MurmurServiceProvider;
use Arttecs\Murmur\Server\Http\Origin;
use Dotenv\Parser\EntryParser;
use Dotenv\Parser\Lines;
use Illuminate\Console\Command;
use Illuminate\Filesystem\Filesystem;
use Illuminate\Routing\Route;
use RuntimeException;
use SensitiveParameter;

/**
 * Install the configuration for Laravel broadcasting through Murmur.
 *
 * @internal
 */
class InstallCommand extends Command
{
    /**
     * The command name and its overwrite option.
     */
    protected $signature = 'murmur:install {--force : Replace the published config and any other broadcast connection}';

    /**
     * The command description shown by Artisan.
     */
    protected $description = 'Configure Laravel broadcasting through Murmur';

    /**
     * Publish the configuration and update any existing environment files.
     */
    public function handle(Filesystem $files): int
    {
        $config = $this->laravel->configPath('murmur.php');
        $source = __DIR__.'/../../../config/murmur.php';
        $force = (bool) $this->option('force');
        $published = ! $files->exists($config) || ($force && $files->hash($config) !== $files->hash($source));

        if ($published) {
            $exit = $this->callSilent('vendor:publish', [
                '--provider' => MurmurServiceProvider::class,
                '--tag' => 'murmur-config',
                '--force' => $force,
            ]);

            if ($exit !== self::SUCCESS || ! $files->isFile($config)) {
                $this->error('Could not publish config/murmur.php.');

                return self::FAILURE;
            }

            $this->info('Published config/murmur.php.');
        }

        $directory = $this->laravel->environmentPath();
        $environment = $this->laravel->environmentFilePath();
        $example = $directory.DIRECTORY_SEPARATOR.'.env.example';
        $updated = false;

        foreach ([$environment, $example] as $path) {
            if ($files->isFile($path) && (! is_readable($path) || ! is_writable($path) || ! is_writable(dirname($path)))) {
                $this->error(basename($path).' is not readable and writable. Nothing was changed in it.');

                return self::FAILURE;
            }
        }

        if ($files->isFile($environment)) {
            $updated = $this->updateEnvironment($files, $environment);
        } else {
            $this->line(basename($environment).' was not found. Add these lines to it and fill MURMUR_SECRET with a randomly generated 64-character hex secret.');
            $this->line('MURMUR_SECRET=');
            $this->line('BROADCAST_CONNECTION=murmur');
        }

        if ($files->isFile($example)) {
            $updated = $this->updateEnvironment($files, $example, example: true) || $updated;
        }

        if (! $updated && ! $published) {
            $this->info('No files changed.');
        }

        $routes = $this->laravel['router']->getRoutes();

        if (! $routes->hasNamedRoute('broadcasting.auth') && ! collect($routes->getRoutes())->contains(
            fn (Route $route): bool => $route->uri() === 'broadcasting/auth' && in_array('POST', $route->methods(), true),
        )) {
            $this->line('For private channels, run php artisan install:broadcasting or call Broadcast::routes().');
        }

        $this->line('Start the server with php artisan murmur:serve.');
        $this->line('Install the JavaScript client with npm install @arttecs/murmur.');
        $this->line('See https://murmur.arttecs.com/docs/ for usage and deployment.');

        return self::SUCCESS;
    }

    /**
     * Set the missing or blank values and write the file only if it changed, keeping its permissions.
     */
    protected function updateEnvironment(Filesystem $files, string $path, bool $example = false): bool
    {
        $original = $files->get($path);
        $content = $this->setEnvironmentValue($original, 'MURMUR_SECRET', fn (): string => $example ? '' : bin2hex(random_bytes(32)));
        $content = $this->setEnvironmentValue(
            $content,
            'BROADCAST_CONNECTION',
            fn (): string => 'murmur',
            fn (string $value): bool => $value !== 'murmur' && (
                $this->option('force') ||
                ($this->input->isInteractive() && $this->confirm('Replace BROADCAST_CONNECTION in '.basename($path).' with murmur?'))
            ),
        );

        if (preg_match('/^(?:export[ \t]+)?BROADCAST_CONNECTION[ \t]*=[ \t]*["\']?murmur["\']?[ \t]*$/m', $content) !== 1) {
            $this->warn('BROADCAST_CONNECTION in '.basename($path).' was left unchanged. Set it to murmur, or run again with --force, to broadcast through Murmur.');
        }

        $origin = $example ? '' : $this->applicationOrigin($original);

        $beforeOrigin = $content;

        if ($origin !== null) {
            $content = $this->setEnvironmentValue($content, 'MURMUR_ALLOWED_ORIGINS', fn (): string => $origin);
        }

        if ($content === $original) {
            return false;
        }

        $files->replace($path, $content, fileperms($path) & 0777);
        $this->info('Updated '.basename($path).'.');

        if ($origin !== null && $origin !== '' && $content !== $beforeOrigin) {
            $this->info("Allowed SSE origin {$origin}.");

            if ($this->isLoopbackOrigin($origin)) {
                $this->line('A development page served on another port (such as a Vite dev server) has a different origin. Add that origin to MURMUR_ALLOWED_ORIGINS.');
            }
        }

        return true;
    }

    /**
     * Change the value of the last assignment for a key or append a new one.
     *
     * @param  callable(): string  $value
     * @param  (callable(string): bool)|null  $overwrite
     */
    protected function setEnvironmentValue(#[SensitiveParameter] string $content, string $key, callable $value, ?callable $overwrite = null): string
    {
        $found = $this->findAssignment($content, $key);

        if ($found === null) {
            preg_match('/\r\n|\n|\r/', $content, $ending);
            $newline = $ending[0] ?? "\n";
            $separator = $content === '' || $content === "\xEF\xBB\xBF" || preg_match('/[\r\n]\z/', $content) === 1 ? '' : $newline;

            return $content.$separator.$key.'='.$value().$newline;
        }

        [$start, $length, $current] = $found;

        if (trim($current ?? '') !== '' && ! ($overwrite && $overwrite($current))) {
            return $content;
        }

        $replacement = $value();

        if ($replacement === '' && trim($current ?? '') === '') {
            return $content;
        }

        $assignment = substr($content, $start, $length);
        $updated = $this->replaceAssignment($assignment, $replacement);

        return substr_replace($content, $updated, $start, $length);
    }

    /**
     * Read the application's serialized origin from the environment file.
     */
    protected function applicationOrigin(#[SensitiveParameter] string $content): ?string
    {
        $url = trim($this->findAssignment($content, 'APP_URL')[2] ?? '');

        if (preg_match('/\A[^\\\\\x00-\x20\x7f]+\z/', $url) !== 1) {
            return null;
        }

        $parts = parse_url($url);

        if (! isset($parts['scheme'], $parts['host'])) {
            return null;
        }

        $scheme = strtolower($parts['scheme']);

        if (! in_array($scheme, ['http', 'https'], true)) {
            return null;
        }

        return Origin::normalize($scheme.'://'.$parts['host'].(isset($parts['port']) ? ':'.$parts['port'] : ''));
    }

    /**
     * Identify localhost and IP loopback origins used for local development.
     */
    protected function isLoopbackOrigin(string $origin): bool
    {
        $host = trim(parse_url($origin, PHP_URL_HOST), '[]');
        $address = inet_pton($host);

        return rtrim($host, '.') === 'localhost'
            || ($address !== false && strlen($address) === 4 && $address[0] === "\x7f")
            || $address === inet_pton('::1');
    }

    /**
     * Find the byte range and parsed value of the last assignment for a key.
     *
     * @return array{int, int, ?string}|null
     */
    protected function findAssignment(#[SensitiveParameter] string $content, string $key): ?array
    {
        $offset = str_starts_with($content, "\xEF\xBB\xBF") ? 3 : 0;
        $lines = preg_split('/\r\n|\n|\r/', substr($content, $offset), flags: PREG_SPLIT_OFFSET_CAPTURE);
        $index = 0;
        $found = null;

        foreach (Lines::process(array_column($lines, 0)) as $raw) {
            $parts = explode("\n", $raw);

            while ($lines[$index][0] !== $parts[0]) {
                $index++;
            }

            $start = $lines[$index][1] + $offset;
            $index += count($parts);
            [$last, $lastOffset] = $lines[$index - 1];
            $parsed = EntryParser::parse($raw)->success()->getOrElse(null);

            if ($parsed?->getName() === $key) {
                $found = [$start, $lastOffset + strlen($last) + $offset - $start, $parsed->getValue()->getOrElse(null)?->getChars()];
            }
        }

        return $found;
    }

    /**
     * Replace the value while preserving spacing, quotes, and any inline comment.
     *
     * @throws RuntimeException
     */
    protected function replaceAssignment(#[SensitiveParameter] string $assignment, #[SensitiveParameter] string $replacement): string
    {
        if (! str_contains($assignment, '=')) {
            return $assignment.'='.$replacement;
        }

        $pattern = <<<'REGEX'
        ~\A(?<prefix>[^=]+=(?:[ \t]*(?=[^# \t\r\n]))?)(?<value>"(?:[^"\\]|\\.)*"|'[^']*'|[^#\r\n]*?)(?<suffix>[ \t]*(?:#[^\r\n]*)?)\z~s
        REGEX;

        if (preg_match($pattern, $assignment, $parts) !== 1) {
            throw new RuntimeException('An existing environment assignment could not be updated.');
        }

        $quote = $parts['value'][0] ?? '';

        if ($quote === '"' || $quote === "'") {
            $replacement = $quote.$replacement.$quote;
        }

        return $parts['prefix'].$replacement.$parts['suffix'];
    }
}
