<?php

use Arttecs\Murmur\Http\Controllers\AuthController;
use Illuminate\Support\Facades\Route;

Route::post(config('murmur.auth.path'), AuthController::class)
    ->middleware(config('murmur.auth.middleware'))
    ->name('murmur.auth');
