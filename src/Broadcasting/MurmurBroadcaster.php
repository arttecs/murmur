<?php

namespace Arttecs\Murmur\Broadcasting;

use Arttecs\Murmur\Exceptions\PresenceNotSupported;
use Arttecs\Murmur\Exceptions\PublishFailed;
use Arttecs\Murmur\MurmurManager;
use Arttecs\Murmur\Server\ConnectionId;
use Arttecs\Murmur\Server\Topic;
use Firebase\JWT\JWT;
use Illuminate\Broadcasting\Broadcasters\Broadcaster;
use Illuminate\Broadcasting\Channel;
use Illuminate\Http\Request;
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
     * Create a broadcaster with the Murmur manager.
     */
    public function __construct(protected MurmurManager $manager) {}

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

        $socket = $payload['socket'] ?? null;
        $except = is_string($socket) && ConnectionId::isValid($socket) ? $socket : null;
        unset($payload['socket']);

        foreach ($topics as $topic) {
            $this->manager->publish($topic, $event, $payload, $except);
        }
    }

    /**
     * Authorize the private channel requested through Laravel's broadcasting endpoint.
     *
     * @param  Request  $request
     * @return array{token: string, expires_at: int}
     *
     * @throws AccessDeniedHttpException
     */
    public function auth($request): array
    {
        $topic = $request->channel_name;

        if (is_string($topic) && str_starts_with($topic, 'presence-')) {
            throw new AccessDeniedHttpException('Murmur does not support presence channels.');
        }

        if (! is_string($topic) || ! Topic::isValid($topic) || ! Topic::isPrivate($topic) || $topic === 'private-') {
            throw new AccessDeniedHttpException;
        }

        $channel = substr($topic, strlen('private-'));

        if ($this->retrieveUser($request, $channel) === null) {
            throw new AccessDeniedHttpException;
        }

        return $this->verifyUserCanAccessChannel($request, $channel);
    }

    /**
     * Issue a token for the one private topic that passed the channel callback.
     *
     * @param  Request  $request
     * @return array{token: string, expires_at: int}
     */
    public function validAuthenticationResponse($request, $result): array
    {
        $token = $this->manager->token([$request->channel_name]);
        [, $encodedClaims] = explode('.', $token);
        $claims = JWT::jsonDecode(JWT::urlsafeB64Decode($encodedClaims));

        return ['token' => $token, 'expires_at' => $claims->exp];
    }
}
