import {
  normalizeRivesRows,
  rivesDayWindows,
  rivesFingerprint,
  RIVES_ET_EAUX_PROVIDER,
  RIVES_ET_EAUX_SCOPE,
  RIVES_ET_EAUX_TIMEZONE,
  RivesEtEauxConnector,
} from '../connectors/rives_et_eaux.js'
import {
  MeterReadingsClient,
  type MeterStreamContext,
  type MeterIngestionBatch,
  type MeterIngestionResult,
} from '../services/meter-readings-client.js'

type RivesClient = {
  getContext: (provider: string, scope: string) => Promise<MeterStreamContext>
  ingest: (batch: MeterIngestionBatch) => Promise<MeterIngestionResult>
}

export function isRivesEtEauxEnabled(): boolean {
  return process.env.RIVES_ET_EAUX_ENABLED === 'true'
}

/** A failed provider window or non-durable PLE acknowledgment fails the BullMQ job. */
export async function pullRivesEtEaux(
  options: {
    enabled?: boolean
    now?: Date
    client?: RivesClient
    connector?: Pick<RivesEtEauxConnector, 'fetchWindow'>
  } = {},
): Promise<void> {
  if (!(options.enabled ?? isRivesEtEauxEnabled())) {
    return
  }

  if (
    (process.env.RIVES_ET_EAUX_TIMEZONE ?? RIVES_ET_EAUX_TIMEZONE) !==
    RIVES_ET_EAUX_TIMEZONE
  ) {
    throw new Error(
      '[rives-et-eaux] RIVES_ET_EAUX_TIMEZONE must be Europe/Paris.',
    )
  }

  const client =
    options.client ??
    new MeterReadingsClient(
      process.env.PLE_BASE_URL ?? '',
      process.env.CLIENT_ID ?? '',
      process.env.CLIENT_SECRET ?? '',
    )
  const context = await client.getContext(
    RIVES_ET_EAUX_PROVIDER,
    RIVES_ET_EAUX_SCOPE,
  )
  if (context.streams.length === 0) {
    console.log('[rives-et-eaux] No authorized active meter streams.')
    return
  }

  const connector =
    options.connector ??
    new RivesEtEauxConnector(
      process.env.RIVES_ET_EAUX_API_KEY ?? '',
      process.env.RIVES_ET_EAUX_BASE_URL,
    )
  const seen = new Set<string>()
  for (const window of rivesDayWindows(options.now)) {
    const rawRows = await connector.fetchWindow(window)
    const fetchedAt = new Date().toISOString()
    const readings = normalizeRivesRows(rawRows, seen)
    const batch: MeterIngestionBatch = {
      provider: RIVES_ET_EAUX_PROVIDER,
      scope: RIVES_ET_EAUX_SCOPE,
      batchId: `rives-et-eaux:${window.localStart}:${rivesFingerprint({readings, fetchedAt})}`,
      windowStart: window.windowStart,
      windowEnd: window.windowEnd,
      fetchedAt,
      complete: true,
      mode: 'LIVE',
      readings,
    }
    const result = await client.ingest(batch)
    console.log(
      `[rives-et-eaux] Durable window ${window.localStart}: ${JSON.stringify(result.counts)}.`,
    )
  }
}
