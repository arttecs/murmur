import { Buffer } from 'node:buffer'
import { spawn } from 'node:child_process'
import { createHmac, randomBytes } from 'node:crypto'
import { createServer, type Server as TcpServer } from 'node:net'
import { fileURLToPath } from 'node:url'
import { expect, vi } from 'vitest'

/** The shared signing secret for this run's test server. */
const secret = randomBytes(32).toString('hex')

/** The CLI entry point for the real Murmur server. */
const serverScript = fileURLToPath(new URL('../../../tests/Support/serve.php', import.meta.url))

/** Binds a local server on an available port. */
export async function listen(server: TcpServer): Promise<number> {
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => {
      server.removeListener('error', reject)
      resolve()
    })
  })
  const address = server.address()
  if (address === null || typeof address === 'string') {
    throw new Error('Expected a local TCP address')
  }
  return address.port
}

/** Stops a Node server and closes its HTTP connections when applicable. */
export function close(server: TcpServer & { closeAllConnections?: () => void }): Promise<void> {
  return new Promise(resolve => {
    server.close(() => resolve())
    server.closeAllConnections?.()
  })
}

/** Starts Murmur and waits for its listening address. */
export async function startMurmur(): Promise<{ address: string; stop: () => Promise<void> }> {
  const socket = createServer()
  let port: number
  try {
    port = await listen(socket)
  } finally {
    await close(socket)
  }

  const child = spawn('php', [serverScript, String(port), secret], { stdio: ['ignore', 'pipe', 'pipe'] })
  const closed = new Promise<void>(resolve => child.once('close', () => resolve()))
  let stdout = ''
  let stderr = ''
  let startupError: Error | undefined
  child.on('error', error => { startupError = error })
  child.stdout.setEncoding('utf8').on('data', (chunk: string) => { stdout += chunk })
  child.stderr.setEncoding('utf8').on('data', (chunk: string) => { stderr += chunk })

  /** Requests graceful shutdown and forces exit if the child stalls. */
  async function stop(): Promise<void> {
    if (child.kill('SIGTERM')) {
      try {
        await vi.waitUntil(() => child.exitCode !== null || child.signalCode !== null, {
          timeout: 1500,
          interval: 20,
        })
      } catch {
        child.kill('SIGKILL')
      }
    }
    await closed
  }

  const address = `http://127.0.0.1:${port}`
  try {
    await vi.waitUntil(() => stdout.includes('\n') || startupError !== undefined ||
      child.exitCode !== null || child.signalCode !== null, { timeout: 5000, interval: 20 })
    expect(stdout.trim()).toBe(address)
    return { address, stop }
  } catch (error) {
    await stop()
    const diagnostic = startupError?.message ?? (stderr.trim() || stdout.trim())
    throw new Error(`Murmur did not start${diagnostic ? ': ' + diagnostic : ''}`, { cause: error })
  }
}

/** Issues a subscription token and its Unix expiration timestamp. */
export function tokenFor(topics: string[], ttl: number): { token: string; expires_at: number } {
  const exp = Math.floor(Date.now() / 1000) + ttl
  const header = Buffer.from(JSON.stringify({ typ: 'JWT', alg: 'HS256' })).toString('base64url')
  const claims = Buffer.from(JSON.stringify({ topics, exp })).toString('base64url')
  const unsigned = `${header}.${claims}`
  const signature = createHmac('sha256', secret).update(unsigned).digest('base64url')
  return { token: `${unsigned}.${signature}`, expires_at: exp }
}

/** Signs a publish request and returns the server-assigned event ID. */
export async function publish(address: string, topic: string, event: string, data: unknown, except?: string): Promise<string> {
  const timestamp = String(Math.floor(Date.now() / 1000))
  const body = JSON.stringify({ topic, event, data, except })
  const signature = createHmac('sha256', secret).update(`${timestamp}.${body}`).digest('hex')
  const response = await fetch(`${address}/publish`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Murmur-Timestamp': timestamp,
      'X-Murmur-Signature': signature,
    },
    body,
    signal: AbortSignal.timeout(3000),
  })
  expect(response.status).toBe(202)
  const result: unknown = await response.json()
  if (result === null || typeof result !== 'object' || !('id' in result) || typeof result.id !== 'string') {
    throw new Error('Murmur returned an invalid publish response')
  }
  expect(result.id).toMatch(/^\d+-\d+$/)
  return result.id
}
