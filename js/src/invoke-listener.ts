/** Calls a listener and surfaces its errors without interrupting other listeners. */
export function invokeListener<Args extends unknown[]>(listener: (...args: Args) => void, ...args: Args): void {
  try {
    listener(...args)
  } catch (error) {
    queueMicrotask(() => { throw error })
  }
}
