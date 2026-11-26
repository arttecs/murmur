import Echo from 'laravel-echo'
import { MurmurConnector } from '../src/echo'
import type { Murmur } from '../src/index'

/** Checks Echo's custom broadcaster constructor and private channel API. */
export function echoPrivateChannel(url: string, callback: (data: unknown) => void): void {
  const echo = new Echo({ broadcaster: MurmurConnector, url })
  echo.private('x').listen('E', callback)
}

/** Checks that the advanced client is typed as Murmur. */
export function echoMurmur(connector: MurmurConnector): Murmur {
  return connector.murmur
}
