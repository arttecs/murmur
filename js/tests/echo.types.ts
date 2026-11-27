import Echo, { type EchoOptions } from 'laravel-echo'
import { MurmurConnector, type MurmurConnectorOptions } from '../src/echo'
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

/** Checks that the connector read through Echo narrows to the typed client. */
export function echoConnector(url: string): Murmur | undefined {
  const connector: unknown = new Echo({ broadcaster: MurmurConnector, url }).connector
  return connector instanceof MurmurConnector ? connector.murmur : undefined
}

/** Checks the documented way to pass options Echo's declarations do not know. */
export function echoWithMurmurOptions(url: string, token: () => string): Echo<'function'> {
  const options: MurmurConnectorOptions = {
    url,
    auth: { credentials: 'omit', headers: () => ({ Authorization: `Bearer ${token()}` }) },
  }

  return new Echo<'function'>({ broadcaster: MurmurConnector, ...options } as EchoOptions<'function'>)
}
