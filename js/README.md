<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/arttecs/murmur/main/art/logo-dark.svg">
    <img src="https://raw.githubusercontent.com/arttecs/murmur/main/art/logo-light.svg" alt="Murmur" width="300">
  </picture>
</p>

# Murmur JavaScript client

Subscribe to public and private Laravel channels over Server-Sent Events with the Murmur JavaScript client.

Install the ESM-only package with npm.

```sh
npm install @arttecs/murmur
```

## Subscribe to a channel

Create a client with your Murmur server's SSE endpoint.

```js
import { Murmur } from '@arttecs/murmur'

const murmur = new Murmur({ url: 'https://events.example.com/sse' })

const news = murmur.channel('news')
news.listen('.updated', data => console.log(data))
```

## Laravel Echo

Install Murmur and Laravel Echo 2.6.3 or a later 2.x release.

```sh
npm install @arttecs/murmur laravel-echo@^2.6.3
```

Pass `MurmurConnector` as the Echo broadcaster.

```js
import Echo from 'laravel-echo'
import { MurmurConnector } from '@arttecs/murmur/echo'

const echo = new Echo({
  broadcaster: MurmurConnector,
  url: 'https://events.example.com/sse',
  authEndpoint: '/broadcasting/auth',
})

echo.private('orders.1')
  .listen('OrderShipped', event => console.log(event))
```

## Documentation

- [JavaScript client](https://murmur.arttecs.com/docs/javascript/)
- [JavaScript authorization](https://murmur.arttecs.com/docs/javascript-authorization/)
- [Laravel Echo connector](https://murmur.arttecs.com/docs/laravel-echo/)

## License

Murmur is released under the [MIT License](https://github.com/arttecs/murmur/blob/main/js/LICENSE).
