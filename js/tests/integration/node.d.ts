/** The Node APIs used by the harness, scoped to the test type-check. */
declare module 'node:child_process' {
  /** A subprocess pipe that delivers UTF-8 output. */
  interface Output {
    setEncoding(encoding: 'utf8'): Output
    on(event: 'data', listener: (chunk: string) => void): void
  }

  /** The pipes and exit state of the PHP subprocess. */
  interface ChildProcess {
    readonly stdout: Output
    readonly stderr: Output
    readonly exitCode: number | null
    readonly signalCode: string | null
    on(event: 'error', listener: (error: Error) => void): void
    once(event: 'close', listener: () => void): void
    kill(signal: 'SIGTERM' | 'SIGKILL'): boolean
  }

  /** Starts a subprocess with its output streams piped to the test runner. */
  export function spawn(command: string, args: string[], options: { stdio: ['ignore', 'pipe', 'pipe'] }): ChildProcess

  /** Runs a command synchronously and exposes a launch error if one occurred. */
  export function spawnSync(command: string, args: string[], options: { timeout: number }): {
    error?: Error & { code?: string }
  }
}

/** The interfaces needed to bind a temporary TCP port. */
declare module 'node:net' {
  /** A TCP server that can bind on loopback and report its port. */
  export interface Server {
    listen(port: number, host: string, callback: () => void): void
    address(): { port: number } | string | null
    close(callback: () => void): void
    once(event: 'error', listener: (error: Error) => void): void
    removeListener(event: 'error', listener: (error: Error) => void): void
  }

  /** Creates a TCP server for finding an available loopback port. */
  export function createServer(): Server
}

/** The interfaces needed for the local token endpoint. */
declare module 'node:http' {
  import type { Server as TcpServer } from 'node:net'

  /** An incoming HTTP request with an asynchronously readable body. */
  interface IncomingMessage {
    readonly method?: string
    readonly url?: string
    setEncoding(encoding: 'utf8'): AsyncIterable<string>
  }

  /** An HTTP response whose headers and body can be written. */
  interface ServerResponse {
    writeHead(status: number, headers?: Record<string, string>): ServerResponse
    end(body?: string): void
  }

  /** An HTTP server that can immediately close active and idle HTTP connections. */
  export interface Server extends TcpServer {
    closeAllConnections(): void
  }

  /** Creates an HTTP server for the provided request handler. */
  export function createServer(listener: (request: IncomingMessage, response: ServerResponse) => void): Server
}

/** The conversions used to encode a JWT. */
declare module 'node:buffer' {
  /** Converts text to bytes and encodes them as base64url. */
  export const Buffer: {
    from(input: string): { toString(encoding: 'base64url'): string }
  }
}

/** The cryptographic operations used to sign HTTP requests and tokens. */
declare module 'node:crypto' {
  /** An HMAC digest that accepts UTF-8 input. */
  interface Hmac {
    update(data: string): Hmac
    digest(encoding: 'hex' | 'base64url'): string
  }

  /** Creates an HMAC signer using the given secret. */
  export function createHmac(algorithm: 'sha256', key: string): Hmac

  /** Generates cryptographically random bytes. */
  export function randomBytes(size: number): { toString(encoding: 'hex'): string }
}

/** The conversion used to locate the PHP fixture. */
declare module 'node:url' {
  /** Converts a file URL to the platform's filesystem path. */
  export function fileURLToPath(url: URL): string
}
