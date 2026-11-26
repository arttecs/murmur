<?php

use Arttecs\Murmur\Console\Commands\ServeCommand;
use Arttecs\Murmur\Contracts\EventLog;
use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Facades\Murmur;
use Arttecs\Murmur\MurmurManager;
use Arttecs\Murmur\Server\Event;
use Arttecs\Murmur\Server\Log\MemoryEventLog;
use Arttecs\Murmur\Tests\Support\FakePublisher;
use Arttecs\Murmur\Tests\Support\ServerHarness;
use Arttecs\Murmur\Tests\Support\SlowReplayLog;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Support\ServiceProvider;
use Symfony\Component\Console\Input\ArrayInput;

it('publishes through a custom driver registered during provider boot', function () {
    config(['murmur.driver' => 'custom_1-driver']);
    $publisher = new FakePublisher;
    $this->app->instance(FakePublisher::class, $publisher);
    $this->app->register(new class($this->app) extends ServiceProvider
    {
        public function boot(): void
        {
            Murmur::extend(
                'custom_1-driver',
                fn (Application $app): Publisher => $app->make(FakePublisher::class),
                fn (Application $app): EventLog => new MemoryEventLog,
            );
        }
    });

    Murmur::publish('news', 'updated', ['count' => 1]);

    expect($publisher->events)->toEqual([new Event('news', 'updated', ['count' => 1])]);
    expect(app(Publisher::class))->toBe($publisher);
});

it('resolves the manager without a valid signing secret until it is needed', function () {
    config(['murmur.secret' => null]);

    expect(app(MurmurManager::class)->publicUrl())->toBe('http://127.0.0.1:8080');
    expect(fn () => Murmur::token(['private-news']))->toThrow(InvalidArgumentException::class, 'at least 32 bytes');
});

it('uses the custom server log and only enables publishing to an in-memory log', function (bool $memory) {
    config(['murmur.driver' => 'custom']);
    $log = $memory ? new MemoryEventLog : new SlowReplayLog(0.01);
    $this->app->instance(EventLog::class, $log);
    Murmur::extend(
        'custom',
        fn (Application $app): Publisher => new FakePublisher,
        fn (Application $app): EventLog => $app->make(EventLog::class),
    );

    $command = app(ServeCommand::class);
    $command->setInput(new ArrayInput([], $command->getDefinition()));
    expect($command->makeLog())->toBe($log);
    expect($command->makeConfig()->publishEnabled)->toBeTrue();

    $h = ServerHarness::start(['publishEnabled' => $command->makeConfig()->publishEnabled], $log);
    expect($h->publish('news', 'updated')->getStatusCode())->toBe($memory ? 202 : 404);
})->with([true, false]);

it('rejects reserved and malformed custom driver names', function (string $driver) {
    expect(fn () => Murmur::extend(
        $driver,
        fn (Application $app): Publisher => new FakePublisher,
        fn (Application $app): EventLog => new MemoryEventLog,
    ))->toThrow(InvalidArgumentException::class);
})->with(['http', 'redis', '', 'CamelCase', 'custom driver', 'custom/driver', "custom\n"]);

it('keeps the unsupported driver errors for both factories', function () {
    config(['murmur.driver' => 'carrier-pigeon']);

    expect(fn () => app(Publisher::class))
        ->toThrow(InvalidArgumentException::class, 'Unsupported Murmur publisher driver [carrier-pigeon].');
    expect(fn () => app(ServeCommand::class)->makeLog())
        ->toThrow(InvalidArgumentException::class, 'Unsupported Murmur server driver [carrier-pigeon].');
});
