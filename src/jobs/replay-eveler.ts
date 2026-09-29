import {
  EvelerClient,
  EvelerConnector,
  evelerWindows,
  type EvelerWindow,
} from '../connectors/eveler.js'
import {PartageonsLeauClient} from '../services/partageonsleau-client.js'

export type ReplayEvelerOptions = {
  connectorId: string
  declarantId: string
  window: EvelerWindow
  apply?: boolean
  cacheDirectory?: string
  cacheOnly?: boolean
}

/** Explicit one-connector replay. No queue/server startup and no other connector runs. */
export async function replayEveler(
  options: ReplayEvelerOptions,
  client = new PartageonsLeauClient(),
  providerClient?: EvelerClient,
) {
  const windows = evelerWindows(options.window)
  if (
    !options.connectorId ||
    !options.declarantId ||
    !process.env.PLE_BASE_URL ||
    !process.env.CLIENT_ID ||
    !process.env.CLIENT_SECRET
  ) {
    throw new Error(
      '[replay-eveler] A connector, declarant and real PLE configuration are required.',
    )
  }
  if (options.cacheOnly && !options.cacheDirectory) {
    throw new Error('[replay-eveler] --cache-only requires --cache-dir.')
  }
  const serviceAccount = process.env.CLIENT_ID
  const token = await client.getServiceAccountToken(serviceAccount)
  const contexts = await client.getContextsForDeclarant(
    options.declarantId,
    token,
  )
  const matches = contexts.flatMap((context) =>
    context.points
      .filter((point) => point.connectorId === options.connectorId)
      .map((point) => ({context, point})),
  )
  if (matches.length !== 1 || matches[0].point.connector !== 'eveler') {
    throw new Error(
      '[replay-eveler] Expected exactly one authorized Eveler connector.',
    )
  }
  const {context, point} = matches[0]
  const provider = options.cacheOnly
    ? undefined
    : (providerClient ?? new EvelerClient())
  const report = {
    mode: options.apply ? 'apply' : 'dry-run',
    windows: 0,
    completeHours: 0,
    incompleteHours: 0,
    invalidRows: 0,
    conflictingRows: 0,
    importedBatches: 0,
    skippedValues: 0,
  }
  for (const window of windows) {
    const connector = new EvelerConnector({
      client: provider,
      window,
      cacheDirectory: options.cacheDirectory,
      cacheOnly: options.cacheOnly,
    })
    const output = await connector.run({
      ...point,
      serviceAccount,
      rate: point.connectorRate,
    })
    const metadata = output.data.source_metadata ?? {}
    report.windows++
    for (const field of [
      'completeHours',
      'incompleteHours',
      'invalidRows',
      'conflictingRows',
    ] as const) {
      report[field] += Number(metadata[field] ?? 0)
    }
    if (
      options.apply &&
      output.data.metrics.some((metric) => metric.values.length > 0)
    ) {
      const acknowledgement = await client.ingest({
        output,
        pointId: point.pointId,
        declarantId: options.declarantId,
        contextId: context.contextId,
        serviceAccountToken: token,
        requireAcknowledgement: true,
      })
      if (!acknowledgement) {
        throw new Error('[replay-eveler] Missing ingestion acknowledgement.')
      }
      report.importedBatches += Number(acknowledgement.imported)
      report.skippedValues += acknowledgement.skippedValues
    }
  }
  return report
}
