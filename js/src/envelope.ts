/** The wrapper the server puts around every event payload. */
export interface Envelope {
  /** The topic the event was published to. */
  topic: string
  /** The event payload. */
  data: unknown
}

/** Parses an event envelope, returning undefined when it is malformed. */
export function parseEnvelope(raw: string): Envelope | undefined {
  let envelope: unknown
  try {
    envelope = JSON.parse(raw)
  } catch {
    return undefined
  }
  if (
    envelope === null || typeof envelope !== 'object' ||
    !('topic' in envelope) || typeof envelope.topic !== 'string' ||
    !('data' in envelope)
  ) {
    return undefined
  }
  return { topic: envelope.topic, data: envelope.data }
}
