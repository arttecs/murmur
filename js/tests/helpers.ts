import { vi } from 'vitest'
import { Murmur, type MurmurOptions } from '../src/index'
import { FakeEventSource } from './fake-event-source'

const clients: Murmur[] = []
const calls: { body: unknown }[] = []

/** Creates a client with a fake EventSource and tracks it for cleanup. */
export function client(options: Partial<MurmurOptions> = {}): Murmur {
  const murmur = new Murmur({
    url: 'https://app.test/sse',
    EventSource: FakeEventSource,
    fetch: () => Promise.reject(new Error('Unexpected token request')),
    ...options,
  })
  clients.push(murmur)
  return murmur
}

/** Closes tracked clients and clears test observations. */
export function reset(): void {
  for (const murmur of clients.splice(0)) {
    murmur.close()
  }
  calls.length = 0
  FakeEventSource.instances.length = 0
}

/** Runs work queued for the current tick. */
export function flush(): Promise<void> {
  return wait(0)
}

/** Advances fake timers and runs pending promise callbacks. */
export async function wait(ms: number): Promise<void> {
  await vi.advanceTimersByTimeAsync(ms)
}

/** Returns the current time in Unix seconds. */
export function nowSeconds(): number {
  return Math.floor(Date.now() / 1000)
}

/** Reads the requested topics from a recorded connection. */
export function topicsOf(source: FakeEventSource): string[] {
  return new URL(source.url).searchParams.get('topics')?.split(',') ?? []
}

/** Creates a successful authorization response and records request bodies. */
export const authFetch = Object.assign(
  (token: { token: string; expires_at: number }) => {
    const fetch: typeof globalThis.fetch = async (_input, init) => {
      const body: unknown = typeof init?.body === 'string' ? JSON.parse(init.body) : undefined
      calls.push({ body })
      return Response.json(token)
    }
    return Object.assign(fetch, { calls })
  },
  { calls },
)

/** Returns a denied authorization once, followed by a valid token. */
export function failingThenOk(): typeof fetch {
  const authorized = authFetch({ token: 'jwt-1', expires_at: nowSeconds() + 900 })
  return vi.fn(authorized).mockResolvedValueOnce(new Response(null, { status: 403 }))
}
