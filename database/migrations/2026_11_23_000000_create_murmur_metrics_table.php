<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Get the database connection used by the metrics store.
     */
    public function getConnection(): ?string
    {
        return config('murmur.metrics.database.connection');
    }

    /**
     * Create the metrics table on the configured connection.
     */
    public function up(): void
    {
        Schema::connection($this->getConnection())->create(config('murmur.metrics.database.table', 'murmur_metrics'), function (Blueprint $table): void {
            $table->string('node', 128)->primary();
            $table->unsignedBigInteger('recorded_at');
            $table->json('samples');
        });
    }

    /**
     * Drop the metrics table from the configured connection.
     */
    public function down(): void
    {
        Schema::connection($this->getConnection())->dropIfExists(config('murmur.metrics.database.table', 'murmur_metrics'));
    }
};
