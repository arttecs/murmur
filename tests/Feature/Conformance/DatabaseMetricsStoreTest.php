<?php

namespace Arttecs\Murmur\Tests\Feature\Conformance;

use Arttecs\Murmur\Contracts\MetricsStore;
use Arttecs\Murmur\Metrics\DatabaseMetricsStore;
use Arttecs\Murmur\Testing\MetricsStoreConformanceTestCase;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\SQLiteConnection;
use PDO;

class DatabaseMetricsStoreTest extends MetricsStoreConformanceTestCase
{
    protected function createMetricsStore(): MetricsStore
    {
        $database = new SQLiteConnection(new PDO('sqlite::memory:'));
        $database->getSchemaBuilder()->create('snapshots', function (Blueprint $table): void {
            $table->string('node')->primary();
            $table->unsignedBigInteger('recorded_at');
            $table->json('samples');
        });

        return new DatabaseMetricsStore($database, 'snapshots');
    }
}
