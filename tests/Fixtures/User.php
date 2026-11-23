<?php

namespace Arttecs\Murmur\Tests\Fixtures;

use Illuminate\Foundation\Auth\User as Authenticatable;

/**
 * Authenticate an in-memory user in feature tests.
 */
class User extends Authenticatable
{
    /**
     * The attributes guarded against mass assignment.
     *
     * @var list<string>
     */
    protected $guarded = [];
}
