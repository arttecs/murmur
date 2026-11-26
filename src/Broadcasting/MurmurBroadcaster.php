<?php

namespace Arttecs\Murmur\Broadcasting;

use Arttecs\Murmur\Contracts\Publisher;
use Arttecs\Murmur\Exceptions\PresenceNotSupported;
use Arttecs\Murmur\Exceptions\PublishFailed;
use Arttecs\Murmur\MurmurManager;
use Arttecs\Murmur\Server\Event;
use Firebase\JWT\JWT;
use Illuminate\Broadcasting\Broadcasters\Broadcaster;
use Illuminate\Broadcasting\Channel;
use Illuminate\Http\Request;
use Illuminate\Support\Str;
use InvalidArgumentException;
use Symfony\Component\HttpKernel\Exception\AccessDeniedHttpException;

/**
 * Publish Laravel broadcasts and authorize private subscriptions through Murmur.
 *
 * @internal
 */
class MurmurBroadcaster extends Broadcaster
{
    /**
     * Create a broadcaster with the Murmur publisher and token manager.
     */
    public function __construct(protected Publisher $publisher, protected MurmurManager $manager) {}

    /**
     * Publish the given event to each channel.
     *
     * @param  array<Channel|string>  $channels
     * @param  string  $event
     * @param  array<string, mixed>  $payload
     *
     * @throws InvalidArgumentException
     * @throws PresenceNotSupported
     * @throws PublishFailed
     */
    public function broadcast(array $channels, $event, array $payload = []): void
    {
        if (str_starts_with($event, 'murmur.')) {
            throw new InvalidArgumentException('Event names starting with "murmur." are reserved.');
        }

        $topics = $this->formatChannels($channels);

        foreach ($topics as $topic) {
            if (str_starts_with($topic, 'presence-')) {
                throw new PresenceNotSupported('Murmur does not support presence channels.');
            }
        }

        unset($payload['socket']);

        foreach ($topics as $topic) {
            $this->publisher->publish(new Event($topic, $event, $payload));
        }
    }

    /**
     * Issue a single subscription token after authorizing every requested channel.
     *
     * @param  list<string>  $channels
     * @return array{token: string, expires_at: int}
     *
     * @throws AccessDeniedHttpException
     */
    public function authorizeChannels(Request $request, array $channels): array
    {
        foreach ($channels as $channel) {
            if (! is_string($channel)) {
                throw new AccessDeniedHttpException;
            }

            $channel = Str::chopStart($channel, 'private-');

            if ($this->retrieveUser($request, $channel) === null) {
                throw new AccessDeniedHttpException;
            }

            $this->verifyUserCanAccessChannel($request, $channel);
        }

        $token = $this->manager->token($channels);
        [, $encodedClaims] = explode('.', $token);
        $claims = JWT::jsonDecode(JWT::urlsafeB64Decode($encodedClaims));

        return ['token' => $token, 'expires_at' => $claims->exp];
    }

    /**
     * Authorize the channel requested through Laravel's broadcasting endpoint.
     *
     * @param  Request  $request
     * @return array{token: string, expires_at: int}
     *
     * @throws AccessDeniedHttpException
     */
    public function auth($request): array
    {
        return $this->authorizeChannels($request, [$request->channel_name]);
    }

    /**
     * Return the result of a successful channel authorization callback.
     *
     * @param  Request  $request
     */
    public function validAuthenticationResponse($request, $result): mixed
    {
        return $result;
    }
}
