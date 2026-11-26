<?php

use Arttecs\Murmur\Broadcasting\MurmurBroadcaster;
use Arttecs\Murmur\MurmurServiceProvider;
use Dotenv\Dotenv;
use Illuminate\Filesystem\Filesystem;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\Broadcast;
use Illuminate\Support\Facades\Route;

beforeEach(function () {
    $this->files = new Filesystem;
    $this->directory = sys_get_temp_dir().'/murmur-install-'.bin2hex(random_bytes(8));
    $this->files->makeDirectory($this->directory);
    $this->app->useEnvironmentPath($this->directory)->useConfigPath($this->directory.'/config');
    (new MurmurServiceProvider($this->app))->boot();
    $this->envFile = $this->directory.'/.env';
    $this->exampleFile = $this->directory.'/.env.example';
    $this->configFile = $this->directory.'/config/murmur.php';
});

afterEach(function () {
    $this->files->deleteDirectory($this->directory);
});

it('publishes configuration and installs a fresh environment without leaking its secret', function () {
    file_put_contents($this->envFile, "APP_NAME=Murmur\n");
    file_put_contents($this->exampleFile, "APP_NAME=Example\n");

    expect(Artisan::call('murmur:install', ['--no-interaction' => true]))->toBe(0);
    $content = file_get_contents($this->envFile);
    $env = Dotenv::parse($content);
    expect($env['MURMUR_SECRET'])->toMatch('/\A[0-9a-f]{64}\z/');
    expect($content)->toBe("APP_NAME=Murmur\nMURMUR_SECRET={$env['MURMUR_SECRET']}\nBROADCAST_CONNECTION=murmur\n");
    expect(file_get_contents($this->exampleFile))->toBe("APP_NAME=Example\nMURMUR_SECRET=\nBROADCAST_CONNECTION=murmur\nMURMUR_ALLOWED_ORIGINS=\n");
    expect(file_get_contents($this->configFile))->toBe(file_get_contents(__DIR__.'/../../config/murmur.php'));
    $output = Artisan::output();
    expect($output)->not->toContain($env['MURMUR_SECRET']);
    expect($output)->toContain('php artisan murmur:serve', 'npm install @arttecs/murmur', 'README');

    config(['murmur.secret' => $env['MURMUR_SECRET']]);
    expect(Broadcast::connection('murmur'))->toBeInstanceOf(MurmurBroadcaster::class);

    expect(Artisan::call('murmur:install', ['--no-interaction' => true]))->toBe(0);
    expect(Artisan::output())->toContain('No files changed');
    expect(file_get_contents($this->envFile))->toBe($content);
    expect(file_get_contents($this->exampleFile))->toBe("APP_NAME=Example\nMURMUR_SECRET=\nBROADCAST_CONNECTION=murmur\nMURMUR_ALLOWED_ORIGINS=\n");
});

it('fills blank assignments without changing other bytes or treating multiline values as variables', function () {
    $original = "OTHER=\"start\r\nMURMUR_SECRET=\r\nend\"\r\n\r\nexport MURMUR_SECRET = \"\"  # set by installer\r\n\"BROADCAST_CONNECTION\"=  # local\r\nLAST=value";
    file_put_contents($this->envFile, $original);
    file_put_contents($this->exampleFile, "MURMUR_SECRET='short-placeholder'\nBROADCAST_CONNECTION=\n");

    expect(Artisan::call('murmur:install', ['--no-interaction' => true]))->toBe(0);
    $content = file_get_contents($this->envFile);
    $secret = Dotenv::parse($content)['MURMUR_SECRET'];
    expect($secret)->toMatch('/\A[0-9a-f]{64}\z/');
    expect($content)->toBe(str_replace(
        ['""  # set', '=  # local'],
        ['"'.$secret.'"  # set', '=murmur  # local'],
        $original,
    ));
    expect(file_get_contents($this->exampleFile))->toBe("MURMUR_SECRET='short-placeholder'\nBROADCAST_CONNECTION=murmur\nMURMUR_ALLOWED_ORIGINS=\n");
});

it('keeps existing secrets, broadcaster and configuration in non-interactive mode', function () {
    $original = "MURMUR_SECRET='existing-secret'\nBROADCAST_CONNECTION=log\n";
    file_put_contents($this->envFile, $original);
    $this->files->ensureDirectoryExists(dirname($this->configFile));
    file_put_contents($this->configFile, '<?php return [];');

    expect(Artisan::call('murmur:install', ['--no-interaction' => true]))->toBe(0);
    expect(file_get_contents($this->envFile))->toBe($original);
    expect(file_get_contents($this->configFile))->toBe('<?php return [];');
    expect(file_exists($this->exampleFile))->toBeFalse();

    expect(Artisan::call('murmur:install', ['--force' => true, '--no-interaction' => true]))->toBe(0);
    expect(file_get_contents($this->envFile))->toBe(str_replace('=log', '=murmur', $original));
    expect(file_get_contents($this->configFile))->toBe(file_get_contents(__DIR__.'/../../config/murmur.php'));
});

it('asks before replacing a different broadcast connection in interactive mode', function (bool $confirm, string $expected) {
    file_put_contents($this->envFile, "MURMUR_SECRET=existing-secret\nBROADCAST_CONNECTION='log' # broadcaster\n");

    $this->artisan('murmur:install')
        ->expectsConfirmation('Replace BROADCAST_CONNECTION in .env with murmur?', $confirm ? 'yes' : 'no')
        ->assertSuccessful();

    expect(file_get_contents($this->envFile))->toBe("MURMUR_SECRET=existing-secret\nBROADCAST_CONNECTION='{$expected}' # broadcaster\n");
})->with([[true, 'murmur'], [false, 'log']]);

it('prints settings to add when the environment file is missing and does not create it', function () {
    expect(Artisan::call('murmur:install', ['--no-interaction' => true]))->toBe(0);
    $output = Artisan::output();
    expect($output)->toContain('.env was not found', 'MURMUR_SECRET=', 'BROADCAST_CONNECTION=murmur');
    expect($output)->not->toMatch('/[a-f0-9]{64}/');
    expect(file_exists($this->envFile))->toBeFalse();
    expect(file_exists($this->configFile))->toBeTrue();
});

it('preserves an application-defined broadcast connection', function () {
    config(['broadcasting.connections.murmur' => ['driver' => 'log', 'custom' => true]]);
    (new MurmurServiceProvider($this->app))->register();

    expect(config('broadcasting.connections.murmur'))->toBe(['driver' => 'log', 'custom' => true]);
});

it('keeps the permissions of the environment file it rewrites', function () {
    file_put_contents($this->envFile, "APP_NAME=Murmur\n");
    chmod($this->envFile, 0600);

    expect(Artisan::call('murmur:install', ['--no-interaction' => true]))->toBe(0);
    clearstatcache();

    expect(file_get_contents($this->envFile))->toContain('MURMUR_SECRET=');
    expect(fileperms($this->envFile) & 0777)->toBe(0600);
});

it('refuses to replace an environment file it cannot write', function () {
    file_put_contents($this->envFile, "APP_NAME=Murmur\n");
    chmod($this->envFile, 0400);

    try {
        expect(Artisan::call('murmur:install', ['--no-interaction' => true]))->toBe(1);
        expect(Artisan::output())->toContain('.env is not readable and writable.');
        expect(file_get_contents($this->envFile))->toBe("APP_NAME=Murmur\n");
        clearstatcache();
        expect(fileperms($this->envFile) & 0777)->toBe(0400);
    } finally {
        chmod($this->envFile, 0600);
    }
})->skip(fn () => function_exists('posix_geteuid') && posix_geteuid() === 0, 'The superuser can write any file.');

it('says when another broadcast connection was left in place', function () {
    file_put_contents($this->envFile, "MURMUR_SECRET=existing\nBROADCAST_CONNECTION=log\n");

    expect(Artisan::call('murmur:install', ['--no-interaction' => true]))->toBe(0);
    expect(Artisan::output())->toContain('BROADCAST_CONNECTION in .env was left unchanged.');

    expect(Artisan::call('murmur:install', ['--force' => true, '--no-interaction' => true]))->toBe(0);
    expect(Artisan::output())->not->toContain('left unchanged');
});

it('allows the application origin in the environment and keeps the example blank', function (?string $assignment) {
    $base = "APP_URL='https://App.test:8443/path?query=1#fragment'\r\nMURMUR_SECRET=existing\r\nBROADCAST_CONNECTION=murmur\r\n";
    file_put_contents($this->envFile, $base.($assignment ?? ''));
    file_put_contents($this->exampleFile, "APP_URL=http://example.test\n");
    chmod($this->envFile, 0600);

    expect(Artisan::call('murmur:install', ['--no-interaction' => true]))->toBe(0);
    expect(Artisan::output())->toContain('Allowed SSE origin https://app.test:8443.')->not->toContain('Vite');
    expect(file_get_contents($this->envFile))->toBe($base.($assignment === null
        ? "MURMUR_ALLOWED_ORIGINS=https://app.test:8443\r\n"
        : 'export MURMUR_ALLOWED_ORIGINS = "https://app.test:8443" # browser'));
    expect(Dotenv::parse(file_get_contents($this->exampleFile))['MURMUR_ALLOWED_ORIGINS'])->toBe('');
    expect(fileperms($this->envFile) & 0777)->toBe(0600);
})->with([[null], ['export MURMUR_ALLOWED_ORIGINS = "" # browser']]);

it('leaves the origin allowlist alone when set or the application URL is invalid', function (string $settings) {
    $original = "MURMUR_SECRET=existing\nBROADCAST_CONNECTION=murmur\n".$settings;
    file_put_contents($this->envFile, $original);

    expect(Artisan::call('murmur:install', ['--no-interaction' => true]))->toBe(0);
    expect(file_get_contents($this->envFile))->toBe($original);
    expect(Artisan::output())->not->toContain('Allowed SSE origin');
})->with([
    "APP_URL=https://app.test\nMURMUR_ALLOWED_ORIGINS=*\n",
    "APP_URL=localhost\n",
    "APP_URL=\n",
    "APP_URL=ftp://app.test:21\n",
    "APP_URL=ws://app.test:80\n",
    "APP_URL=wss://app.test:443\n",
]);

it('prints one instruction when the application has no broadcasting auth route', function () {
    expect(Artisan::call('murmur:install', ['--no-interaction' => true]))->toBe(0);
    $output = Artisan::output();
    expect(substr_count($output, 'php artisan install:broadcasting'))->toBe(1);
    expect($output)->toContain('Broadcast::routes()');
    expect(Route::getRoutes()->getRoutesByMethod()['POST']['broadcasting/auth'] ?? null)->toBeNull();
});

it('omits the instruction when laravel registered its unnamed broadcasting auth route', function () {
    Broadcast::routes();

    expect(Artisan::call('murmur:install', ['--no-interaction' => true]))->toBe(0);
    expect(Artisan::output())->not->toContain('php artisan install:broadcasting');
});

it('prints one development port hint when adding a loopback origin', function (string $url, string $origin) {
    file_put_contents($this->envFile, "APP_URL={$url}\nMURMUR_SECRET=existing\nBROADCAST_CONNECTION=murmur\n");

    expect(Artisan::call('murmur:install', ['--no-interaction' => true]))->toBe(0);
    $output = Artisan::output();
    expect(Dotenv::parse(file_get_contents($this->envFile))['MURMUR_ALLOWED_ORIGINS'])->toBe($origin);
    expect(substr_count($output, 'Vite'))->toBe(1);
    expect($output)->toContain('different origin', 'MURMUR_ALLOWED_ORIGINS');

    expect(Artisan::call('murmur:install', ['--no-interaction' => true]))->toBe(0);
    expect(Artisan::output())->not->toContain('Vite');
})->with([
    ['HTTP://Localhost:80/path', 'http://localhost'],
    ['http://127.0.0.2:5173', 'http://127.0.0.2:5173'],
    ['https://[::1]:443', 'https://[::1]'],
    ['http://[0:0:0:0:0:0:0:1]', 'http://[0:0:0:0:0:0:0:1]'],
]);
