<?php

namespace Arttecs\Murmur\Tests\Feature\Conformance;

use Arttecs\Murmur\Contracts\MetricsStore;
use Arttecs\Murmur\Metrics\CacheMetricsStore;
use Arttecs\Murmur\Testing\MetricsStoreConformanceTestCase;
use Illuminate\Cache\ArrayStore;
use Illuminate\Cache\Repository;

class CacheMetricsStoreTest extends MetricsStoreConformanceTestCase
{
    protected function createMetricsStore(): MetricsStore
    {
        return new CacheMetricsStore(new Repository(new ArrayStore), 'test:', 60);
    }
}
