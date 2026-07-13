import process from 'node:process'
import path from 'node:path'
import {tmpdir} from 'node:os'
import {Buffer} from 'node:buffer'
import {mkdtemp, rm, writeFile} from 'node:fs/promises'
import {connectorRegistry} from '../connectors/index.js'
import {parseAquasysSourcePointId} from '../connectors/aquasys.js'
import {
  ConflictPolicy,
  type ConnectorOutput,
  type ConnectorSourceFile,
  Granularity,
  MetricType,
  PointFlowType,
  type ParsedPointPayload,
  type Timeserie,
  type WaterUseCode,
} from '../connectors/types.js'

type DeclarationFile = {
  id: string
  type: string
  filename: string
  url: string
}

type DeclarationPoint = {
  pointId: string
  name: string
  flowType?: PointFlowType
  sourceId?: string | undefined
  sourcePointId?: string
  mostRecentAvailableDate?: string | undefined
}

type DeclarationProcessingContext = {
  id: string
  code?: string
  type: string
  declarantUserId: string
  autoValidationEnabled?: boolean
  files: DeclarationFile[]
  points: DeclarationPoint[]
}

type ServiceAccountTokenResponse = {
  accessToken?: string
  access_token?: string
  token?: string
}

type LocalDeclarationFile = DeclarationFile & {
  localPath: string
}

type LegacySeriesValue = {
  date: string
  periodStart?: string
  periodEnd?: string
  value: number
}

type LegacySeries = {
  pointPrelevement: string
  flowType?: PointFlowType
  usage?: WaterUseCode
  metadata?: Record<string, unknown>
  parameter: string
  unit: string | undefined
  frequency: string
  minDate: string
  maxDate: string
  data: LegacySeriesValue[]
}

type LegacyIngestionPayload = {
  conflictPolicy: ConflictPolicy
  series: LegacySeries[]
}

type IngestionResult = {
  errors: unknown[]
  data: LegacyIngestionPayload | undefined
}

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function isDeclarationFile(value: unknown): value is DeclarationFile {
  if (!isObjectRecord(value)) {
    return false
  }

  return (
    typeof value.id === 'string' &&
    typeof value.type === 'string' &&
    typeof value.filename === 'string' &&
    typeof value.url === 'string'
  )
}

function isDeclarationPoint(value: unknown): value is DeclarationPoint {
  if (!isObjectRecord(value)) {
    return false
  }

  if (typeof value.pointId !== 'string' || typeof value.name !== 'string') {
    return false
  }

  if (
    value.flowType !== undefined &&
    value.flowType !== PointFlowType.PRELEVEMENT &&
    value.flowType !== PointFlowType.REJET
  ) {
    return false
  }

  if (value.sourceId !== undefined && typeof value.sourceId !== 'string') {
    return false
  }

  if (
    value.sourcePointId !== undefined &&
    typeof value.sourcePointId !== 'string'
  ) {
    return false
  }

  return (
    value.mostRecentAvailableDate === undefined ||
    typeof value.mostRecentAvailableDate === 'string'
  )
}

function isDeclarationProcessingContext(
  value: unknown,
): value is DeclarationProcessingContext {
  if (!isObjectRecord(value)) {
    return false
  }

  if (
    typeof value.id !== 'string' ||
    (value.code !== undefined && typeof value.code !== 'string') ||
    typeof value.type !== 'string' ||
    typeof value.declarantUserId !== 'string'
  ) {
    return false
  }

  if (
    value.autoValidationEnabled !== undefined &&
    typeof value.autoValidationEnabled !== 'boolean'
  ) {
    return false
  }

  return (
    Array.isArray(value.files) &&
    value.files.every((file) => isDeclarationFile(file)) &&
    Array.isArray(value.points) &&
    value.points.every((point) => isDeclarationPoint(point))
  )
}

function isServiceAccountTokenResponse(
  value: unknown,
): value is ServiceAccountTokenResponse {
  if (!isObjectRecord(value)) {
    return false
  }

  const {accessToken, access_token: accessTokenUnderscore, token} = value

  return (
    (accessToken === undefined || typeof accessToken === 'string') &&
    (accessTokenUnderscore === undefined ||
      typeof accessTokenUnderscore === 'string') &&
    (token === undefined || typeof token === 'string')
  )
}

function getRequiredEnv(name: string): string {
  const value = process.env[name]

  if (!value) {
    throw new Error(`[process-declaration] Missing ${name}`)
  }

  return value
}

function toIsoDate(date: Date): string {
  return date.toISOString().slice(0, 10)
}

function toLegacyValueDate(date: Date, granularity: Granularity): string {
  return granularity === Granularity.DAY ? toIsoDate(date) : date.toISOString()
}

function metricTypeToParameter(metricType: MetricType): string {
  switch (metricType) {
    case MetricType.VOLUME: {
      return 'volume'
    }

    case MetricType.DEBIT: {
      return 'débit'
    }

    case MetricType.INDEX: {
      return 'index'
    }
  }
}

function sanitizeFilename(filename: string): string {
  return path.basename(filename || 'file').replaceAll(/[^\w.\-]+/gv, '_')
}

function resolveConnectorName(declarationType: string): string | undefined {
  switch (declarationType) {
    case 'template-file': {
      return 'template_file'
    }

    case 'aquasys':
    case 'extract-aquasys': {
      return 'aquasys'
    }

    case 'bv-tech': {
      return 'bv_tech'
    }

    case 'gidaf': {
      return 'gidaf'
    }

    default: {
      return undefined
    }
  }
}

function resolveSourcePointId(parameters: {
  connectorName: string
  point: DeclarationPoint
}): string {
  const {connectorName, point} = parameters

  if (point.sourcePointId) {
    return point.sourcePointId
  }

  if (connectorName === 'template_file') {
    return point.name
  }

  if (connectorName === 'aquasys') {
    return point.name
  }

  if (connectorName === 'bv_tech') {
    return point.name
  }

  if (connectorName === 'gidaf') {
    return point.sourceId ?? point.sourcePointId ?? point.name
  }

  return point.pointId
}

function selectFilesForConnector(parameters: {
  declarationType: string
  files: LocalDeclarationFile[]
}): LocalDeclarationFile[] {
  const {declarationType, files} = parameters

  switch (declarationType) {
    case 'template-file': {
      const templateFiles = files.filter(
        (file) => file.type === 'template-file',
      )

      return templateFiles.length > 0 ? templateFiles : files
    }

    case 'bv-tech': {
      const bvTechFiles = files.filter((file) => file.type === 'bv-tech')
      return bvTechFiles.length > 0 ? bvTechFiles : files
    }

    case 'aquasys':
    case 'extract-aquasys': {
      const aquasysFiles = files.filter(
        (file) => file.type === 'aquasys' || file.type === 'extract-aquasys',
      )

      return aquasysFiles.length > 0 ? aquasysFiles : files
    }

    case 'gidaf': {
      const gidafFiles = files.filter((file) =>
        ['gidaf', 'gidaf-cadres', 'gidaf-prelevements'].includes(file.type),
      )

      return gidafFiles.length > 0 ? gidafFiles : files
    }

    default: {
      return []
    }
  }
}

function toConnectorSourceFiles(
  files: LocalDeclarationFile[],
): ConnectorSourceFile[] {
  return files.map((file) => ({
    type: file.type,
    filename: file.filename,
    path: file.localPath,
  }))
}

function buildConnectorFileBatches(parameters: {
  connectorName: string
  sourceFiles: LocalDeclarationFile[]
}): Array<{
  sourceFile: LocalDeclarationFile | undefined
  sourceFiles: LocalDeclarationFile[]
}> {
  const {connectorName, sourceFiles} = parameters

  if (connectorName === 'gidaf') {
    return [
      {
        sourceFile: undefined,
        sourceFiles,
      },
    ]
  }

  return sourceFiles.map((sourceFile) => ({
    sourceFile,
    sourceFiles: [sourceFile],
  }))
}

function normalizeSourcePointKey(value: string): string {
  return value
    .normalize('NFD')
    .replaceAll(/[\u0300-\u036F]/gv, '')
    .trim()
    .replaceAll(/\s+/gv, ' ')
    .toLowerCase()
}

function dedupeSourcePointIds(sourcePointIds: string[]): string[] {
  const seen = new Set<string>()
  const uniqueSourcePointIds: string[] = []

  for (const sourcePointId of sourcePointIds) {
    const normalizedSourcePointId = sourcePointId
      .trim()
      .replaceAll(/\s+/gv, ' ')
    const key = normalizeSourcePointKey(normalizedSourcePointId)

    if (!key || seen.has(key)) {
      continue
    }

    seen.add(key)
    uniqueSourcePointIds.push(normalizedSourcePointId)
  }

  return uniqueSourcePointIds
}

function getPointLookupKeys(parameters: {
  connectorName: string
  point: DeclarationPoint
}): string[] {
  const {connectorName, point} = parameters
  const values = [
    resolveSourcePointId({connectorName, point}),
    point.name,
    point.sourceId,
    point.sourcePointId,
  ]

  return values
    .filter(
      (value): value is string =>
        typeof value === 'string' && value.trim().length > 0,
    )
    .map((value) => normalizeSourcePointKey(value))
}

function buildDeclarationPointForDetectedSourceId(parameters: {
  connectorName: string
  declarationPoints: DeclarationPoint[]
  sourcePointId: string
}): DeclarationPoint {
  const {connectorName, declarationPoints, sourcePointId} = parameters
  const pointName =
    connectorName === 'aquasys'
      ? parseAquasysSourcePointId(sourcePointId).sourcePointId
      : sourcePointId
  const sourcePointKey = normalizeSourcePointKey(pointName)

  for (const point of declarationPoints) {
    if (getPointLookupKeys({connectorName, point}).includes(sourcePointKey)) {
      return {
        ...point,
        sourcePointId,
      }
    }
  }

  return {
    pointId: `detected:${sourcePointId}`,
    name: pointName,
    sourcePointId,
  }
}

function buildPointsForDetectedSourceIds(parameters: {
  connectorName: string
  declarationPoints: DeclarationPoint[]
  detectedSourcePointIds: string[]
}): DeclarationPoint[] {
  const {connectorName, declarationPoints, detectedSourcePointIds} = parameters
  const uniqueSourcePointIds = dedupeSourcePointIds(detectedSourcePointIds)
  const points: DeclarationPoint[] = []
  const seenRunKeys = new Set<string>()

  for (const sourcePointId of uniqueSourcePointIds) {
    const point = buildDeclarationPointForDetectedSourceId({
      connectorName,
      declarationPoints,
      sourcePointId,
    })
    const runKey = normalizeSourcePointKey(
      resolveSourcePointId({connectorName, point}),
    )

    if (seenRunKeys.has(runKey)) {
      continue
    }

    seenRunKeys.add(runKey)
    points.push(point)
  }

  return points
}

function hasValues(output: ConnectorOutput): boolean {
  return output.data.metrics.some((metric) => metric.values.length > 0)
}

function metricToLegacySeries(parameters: {
  point: DeclarationPoint
  metric: Timeserie
  payload: ParsedPointPayload
}): LegacySeries | undefined {
  const {point, metric, payload} = parameters

  const values = metric.values
    .filter((value) => Number.isFinite(value.value))
    .map((value) => ({
      date: toLegacyValueDate(value.date, metric.granularity),
      ...(value.periodStart
        ? {periodStart: value.periodStart.toISOString()}
        : {}),
      ...(value.periodEnd ? {periodEnd: value.periodEnd.toISOString()} : {}),
      value: value.value,
    }))

  if (values.length === 0) {
    return undefined
  }

  // eslint-disable-next-line unicorn/no-array-sort
  const sortedValues = [...values].sort((left, right) =>
    (left.periodStart ?? left.date).localeCompare(
      right.periodStart ?? right.date,
    ),
  )

  return {
    pointPrelevement: point.name,
    ...((payload.flow_type ?? point.flowType)
      ? {flowType: payload.flow_type ?? point.flowType}
      : {}),
    ...(metric.usage ? {usage: metric.usage} : {}),
    ...(payload.source_metadata ? {metadata: payload.source_metadata} : {}),
    parameter: metricTypeToParameter(metric.type),
    unit: metric.unit,
    frequency: metric.granularity,
    minDate: payload.min_date
      ? toIsoDate(payload.min_date)
      : sortedValues[0].date,
    maxDate: payload.max_date
      ? toIsoDate(payload.max_date)
      : sortedValues.at(-1)!.date,
    data: sortedValues,
  }
}

function connectorOutputsToLegacyPayload(parameters: {
  connectorName: string
  outputs: Array<{
    point: DeclarationPoint
    output: ConnectorOutput
  }>
}): LegacyIngestionPayload {
  const series: LegacySeries[] = []
  let hasOnlyPunctualMetrics = parameters.outputs.length > 0
  const configuredConflictPolicies = new Set<ConflictPolicy>()

  for (const {point, output} of parameters.outputs) {
    if (
      output.data.metrics.length === 0 ||
      output.data.metrics.some(
        (metric) => metric.granularity !== Granularity.FIFTEEN_MINUTES,
      )
    ) {
      hasOnlyPunctualMetrics = false
    }

    for (const metric of output.data.metrics) {
      configuredConflictPolicies.add(metric.conflictPolicy)

      const legacySeries = metricToLegacySeries({
        point,
        metric,
        payload: output.data,
      })

      if (legacySeries) {
        series.push(legacySeries)
      }
    }
  }

  const hasVolumeSeries = series.some(
    (item) => item.parameter === metricTypeToParameter(MetricType.VOLUME),
  )

  if (configuredConflictPolicies.size === 1) {
    return {
      conflictPolicy: [...configuredConflictPolicies][0],
      series,
    }
  }

  return {
    conflictPolicy: hasOnlyPunctualMetrics
      ? ConflictPolicy.REPLACE_EXISTING
      : hasVolumeSeries
        ? ConflictPolicy.SKIP_NEW_CHUNK
        : ConflictPolicy.REPLACE_EXISTING,
    series,
  }
}

async function getServiceAccountToken(): Promise<string> {
  const pleBaseUrl = getRequiredEnv('PLE_BASE_URL')
  const clientId = getRequiredEnv('CLIENT_ID')
  const clientSecret = getRequiredEnv('CLIENT_SECRET')

  const response = await fetch(`${pleBaseUrl}/service-accounts/token`, {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      clientId,
      clientSecret,
    }),
  })

  if (!response.ok) {
    throw new Error(
      `[process-declaration] PLE auth failed with status ${response.status}: ${await response.text()}`,
    )
  }

  const body: unknown = await response.json()

  if (!isServiceAccountTokenResponse(body)) {
    throw new Error('[process-declaration] Invalid token response')
  }

  const token = body.accessToken ?? body.access_token ?? body.token

  if (!token) {
    throw new Error(
      '[process-declaration] Missing access token in PLE response',
    )
  }

  return token
}

async function getDeclarationProcessingContext(
  declarationId: string,
  token: string,
): Promise<DeclarationProcessingContext> {
  const pleBaseUrl = getRequiredEnv('PLE_BASE_URL')

  const response = await fetch(
    `${pleBaseUrl}/service-accounts/declarations/${encodeURIComponent(
      declarationId,
    )}/processing-context`,
    {
      headers: {
        Accept: 'application/json',
        Authorization: `Bearer ${token}`,
      },
    },
  )

  if (!response.ok) {
    throw new Error(
      `[process-declaration] GET declaration processing context failed with status ${response.status}: ${await response.text()}`,
    )
  }

  const body: unknown = await response.json()

  if (!isObjectRecord(body) || body.success !== true) {
    throw new Error(
      '[process-declaration] Invalid declaration context response',
    )
  }

  if (!isDeclarationProcessingContext(body.data)) {
    throw new TypeError(
      '[process-declaration] Invalid declaration context data',
    )
  }

  return body.data
}

async function downloadFileToTemporary(parameters: {
  file: DeclarationFile
  directory: string
}): Promise<LocalDeclarationFile> {
  const {file, directory} = parameters

  const response = await fetch(file.url)

  if (!response.ok) {
    throw new Error(
      `[process-declaration] Download failed for ${file.filename} with status ${response.status}`,
    )
  }

  const buffer = Buffer.from(await response.arrayBuffer())
  const filename = sanitizeFilename(file.filename)
  const localPath = path.join(directory, `${file.id}-${filename}`)

  await writeFile(localPath, buffer)

  return {
    ...file,
    localPath,
  }
}

async function downloadDeclarationFilesToTemporary(parameters: {
  declaration: DeclarationProcessingContext
  directory: string
}): Promise<LocalDeclarationFile[]> {
  const {declaration, directory} = parameters

  return Promise.all(
    declaration.files.map(async (file) =>
      downloadFileToTemporary({
        file,
        directory,
      }),
    ),
  )
}

async function runConnectorForDeclaration(parameters: {
  declaration: DeclarationProcessingContext
  localFiles: LocalDeclarationFile[]
}): Promise<IngestionResult> {
  const {declaration, localFiles} = parameters
  const errors: unknown[] = []

  const connectorName = resolveConnectorName(declaration.type)

  if (!connectorName) {
    return {
      errors: [
        `Type de déclaration non supporté par les connecteurs: ${declaration.type}`,
      ],
      data: undefined,
    }
  }

  const connector = connectorRegistry.get(connectorName)

  if (!connector) {
    return {
      errors: [`Connecteur introuvable: ${connectorName}`],
      data: undefined,
    }
  }

  const sourceFiles = selectFilesForConnector({
    declarationType: declaration.type,
    files: localFiles,
  })

  if (sourceFiles.length === 0) {
    return {
      errors: [
        `Aucun fichier exploitable pour la déclaration ${declaration.id}`,
      ],
      data: undefined,
    }
  }

  const outputs: Array<{
    point: DeclarationPoint
    output: ConnectorOutput
  }> = []

  const fileBatches = buildConnectorFileBatches({
    connectorName,
    sourceFiles,
  })

  for (const fileBatch of fileBatches) {
    let pointsToProcess = declaration.points

    try {
      const detectedSourcePointIds = await connector.discoverSourcePointIds({
        sourceFile: fileBatch.sourceFile?.localPath,
        sourceFiles: toConnectorSourceFiles(fileBatch.sourceFiles),
      })

      if (detectedSourcePointIds.length > 0) {
        pointsToProcess = buildPointsForDetectedSourceIds({
          connectorName,
          declarationPoints: declaration.points,
          detectedSourcePointIds,
        })
      }

      console.log(
        `[process-declaration] ${connectorName} detected source points=${detectedSourcePointIds.length}, processed points=${pointsToProcess.length}`,
      )
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)

      errors.push({
        fileId:
          fileBatch.sourceFile?.id ??
          fileBatch.sourceFiles.map((file) => file.id).join(','),
        filename:
          fileBatch.sourceFile?.filename ??
          fileBatch.sourceFiles.map((file) => file.filename).join(', '),
        connector: connectorName,
        error: message,
      })
    }

    for (const point of pointsToProcess) {
      const sourcePointId = resolveSourcePointId({
        connectorName,
        point,
      })

      try {
        const output = await connector.run({
          serviceAccount: 'declaration-upload',
          flowType: point.flowType,
          sourcePointId,
          rate: 100,
          sourceFile: fileBatch.sourceFile?.localPath,
          sourceFiles: toConnectorSourceFiles(fileBatch.sourceFiles),
          // Pour une déclaration, on veut relire tout le fichier.
          // On ne veut pas filtrer à partir de la date d'activation du connecteur.
          mostRecentAvailableDate: new Date('1900-01-01T00:00:00.000Z'),
        })

        if (!hasValues(output)) {
          continue
        }

        outputs.push({
          point,
          output,
        })
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error)

        errors.push({
          pointId: point.pointId,
          pointName: point.name,
          fileId:
            fileBatch.sourceFile?.id ??
            fileBatch.sourceFiles.map((file) => file.id).join(','),
          filename:
            fileBatch.sourceFile?.filename ??
            fileBatch.sourceFiles.map((file) => file.filename).join(', '),
          connector: connectorName,
          error: message,
        })
      }
    }
  }

  const data = connectorOutputsToLegacyPayload({
    connectorName,
    outputs,
  })

  return {
    errors,
    data: data.series.length > 0 ? data : undefined,
  }
}

async function postIngestionResult(parameters: {
  declarationId: string
  token: string
  result: IngestionResult
}): Promise<void> {
  const pleBaseUrl = getRequiredEnv('PLE_BASE_URL')

  const response = await fetch(
    `${pleBaseUrl}/service-accounts/declarations/${encodeURIComponent(
      parameters.declarationId,
    )}/ingest`,
    {
      method: 'POST',
      headers: {
        Accept: 'application/json',
        Authorization: `Bearer ${parameters.token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        errors: parameters.result.errors,
        data: parameters.result.data ?? {
          conflictPolicy: ConflictPolicy.REPLACE_EXISTING,
          series: [],
        },
      }),
    },
  )

  if (!response.ok) {
    throw new Error(
      `[process-declaration] POST declaration ingest failed with status ${response.status}: ${await response.text()}`,
    )
  }
}

type ProcessDeclarationResult = {
  declarationId: string
  declarationCode: string | undefined
  seriesCount: number
  errorCount: number
}

export async function processDeclaration(
  declarationIdentifier: string,
): Promise<ProcessDeclarationResult> {
  console.log(
    `[process-declaration] Processing declaration ${declarationIdentifier}`,
  )

  const token = await getServiceAccountToken()
  const declaration = await getDeclarationProcessingContext(
    declarationIdentifier,
    token,
  )

  console.log(
    `[process-declaration] Declaration ${declaration.id}, code=${declaration.code ?? 'n/a'}, type=${declaration.type}, files=${declaration.files.length}, points=${declaration.points.length}`,
  )

  const temporaryDirectory = await mkdtemp(
    path.join(tmpdir(), `ple-declaration-${declaration.id}-`),
  )

  try {
    const localFiles = await downloadDeclarationFilesToTemporary({
      declaration,
      directory: temporaryDirectory,
    })

    const result = await runConnectorForDeclaration({
      declaration,
      localFiles,
    })

    await postIngestionResult({
      declarationId: declaration.id,
      token,
      result,
    })

    console.log(
      `[process-declaration] Declaration ${declaration.id} ingested: series=${result.data?.series.length ?? 0}, errors=${result.errors.length}`,
    )

    return {
      declarationId: declaration.id,
      declarationCode: declaration.code,
      seriesCount: result.data?.series.length ?? 0,
      errorCount: result.errors.length,
    }
  } finally {
    await rm(temporaryDirectory, {
      recursive: true,
      force: true,
    })
  }
}
