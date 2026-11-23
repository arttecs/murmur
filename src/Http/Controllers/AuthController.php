<?php

namespace Arttecs\Murmur\Http\Controllers;

use Arttecs\Murmur\Server\Topic;
use Closure;
use Illuminate\Broadcasting\BroadcastManager;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Validator;
use Symfony\Component\HttpKernel\Exception\AccessDeniedHttpException;

/**
 * Exchange authenticated channel access for a Murmur subscription token.
 */
class AuthController
{
    /**
     * Validate and authorize a request for private channel subscriptions.
     *
     * @throws AccessDeniedHttpException
     */
    public function __invoke(Request $request, BroadcastManager $broadcast): JsonResponse
    {
        $validator = Validator::make($request->all(), [
            'channels' => ['required', 'array', 'list', 'max:'.config('murmur.server.max_topics_per_connection')],
            'channels.*' => ['required', 'string', 'starts_with:private-', function (string $attribute, mixed $value, Closure $fail): void {
                if (! is_string($value) || ! Topic::isValid($value)) {
                    $fail("The {$attribute} field must be a valid channel name.");
                }
            }],
        ]);

        $validator->after(function (\Illuminate\Validation\Validator $validator): void {
            if ($validator->errors()->isEmpty() && array_sum(array_map(strlen(...), $validator->getData()['channels'])) > 2048) {
                $validator->errors()->add('channels', 'The channel names must total at most 2048 bytes.');
            }
        });

        if ($validator->fails()) {
            return response()->json([
                'message' => $validator->errors()->first(),
                'errors' => $validator->errors(),
            ], 422);
        }

        return response()->json(
            $broadcast->connection('murmur')->authorizeChannels($request, $validator->validated()['channels']),
        );
    }
}
