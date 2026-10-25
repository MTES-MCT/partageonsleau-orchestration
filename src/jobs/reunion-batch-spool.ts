import {Buffer} from 'node:buffer'
import {createHash} from 'node:crypto'
import path from 'node:path'
import {writeFile} from 'node:fs/promises'
import type {NativeSeries} from '../connectors/reunion-types.js'
import {ConflictPolicy} from '../connectors/types.js'

export const REUNION_BATCH_TARGET_BYTES = 16 * 1024 * 1024
export const REUNION_BATCH_MAX_BYTES = 48 * 1024 * 1024
export const REUNION_BATCH_TRANSPORT_VERSION = 'whole-series-spool-v1'

export type NativeSpoolPart = {
  path: string
  checksum: string
  bytes: number
  seriesCount: number
  errorCount: number
}

export type NativeSpoolPayload = {
  errors: unknown[]
  data: {
    nativeTemporal: true
    conflictPolicy: ConflictPolicy
    batch?: {id: string; index: number; total: number}
    series: NativeSeries[]
  }
}

const prefix = '{"errors":['
const middle = `],"data":{"nativeTemporal":true,"conflictPolicy":"${ConflictPolicy.SKIP_NEW_CHUNK}","series":[`
const suffix = ']}}'
// The final 64-character hash and index/total (each <= 10000) are added after
// every file has been prepared. Leave more than their maximum JSON overhead.
const BATCH_RESERVE_BYTES = 256
const EMPTY_BYTES = Buffer.byteLength(prefix + middle + suffix)

/** Keep whole series and every error intact. No API request occurs while spooling. */
export async function spoolNativeFileParts({
  series,
  errors,
  directory,
  fileIndex,
}: {
  series: NativeSeries[]
  errors: unknown[]
  directory: string
  fileIndex: number
}): Promise<NativeSpoolPart[]> {
  const parts: NativeSpoolPart[] = []
  let seriesJson: string[] = []
  let errorsJson: string[] = []
  let bodyBytes = EMPTY_BYTES

  async function flush(): Promise<void> {
    const body = `${prefix}${errorsJson.join(',')}${middle}${seriesJson.join(',')}${suffix}`
    const bytes = Buffer.byteLength(body)
    if (bytes + BATCH_RESERVE_BYTES > REUNION_BATCH_MAX_BYTES)
      throw new Error('Une partie native dépasse la limite sûre de 48 MiB.')
    const filename = path.join(
      directory,
      `file-${fileIndex}-part-${parts.length}.json`,
    )
    await writeFile(filename, body, {flag: 'wx', mode: 0o600})
    parts.push({
      path: filename,
      checksum: createHash('sha256').update(body).digest('hex'),
      bytes,
      seriesCount: seriesJson.length,
      errorCount: errorsJson.length,
    })
    seriesJson = []
    errorsJson = []
    bodyBytes = EMPTY_BYTES
  }

  async function append(value: unknown, kind: 'series' | 'errors') {
    const json = JSON.stringify(value)
    if (json === undefined)
      throw new Error('Une donnée native ne peut pas être sérialisée.')
    const bytes = Buffer.byteLength(json)
    if (EMPTY_BYTES + bytes + BATCH_RESERVE_BYTES > REUNION_BATCH_MAX_BYTES)
      throw new Error(
        kind === 'series'
          ? 'Une série native dépasse 48 MiB : aucun envoi effectué. Le fichier original est conservé ; un découpage explicite de série est nécessaire.'
          : 'Une erreur de parsing dépasse 48 MiB : aucun envoi effectué. Le fichier original est conservé.',
      )
    const current = kind === 'series' ? seriesJson : errorsJson
    const commaBytes = current.length > 0 ? 1 : 0
    if (
      (seriesJson.length > 0 || errorsJson.length > 0) &&
      bodyBytes + bytes + commaBytes + BATCH_RESERVE_BYTES >
        REUNION_BATCH_TARGET_BYTES
    )
      await flush()
    const target = kind === 'series' ? seriesJson : errorsJson
    bodyBytes += bytes + (target.length > 0 ? 1 : 0)
    target.push(json)
  }

  // Errors precede their series, so cumulative parsing diagnostics are already
  // available to the API when it imports the following series from this file.
  for (const error of errors) await append(error, 'errors')
  for (const item of series) await append(item, 'series')
  if (seriesJson.length || errorsJson.length || parts.length === 0)
    await flush()
  return parts
}
