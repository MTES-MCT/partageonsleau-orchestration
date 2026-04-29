import process from 'node:process'
import path from 'node:path'
import {tmpdir} from 'node:os'
import {Buffer} from 'node:buffer'
import {mkdtemp, rm, writeFile} from 'node:fs/promises'
import {connectorRegistry} from '../connectors/index.js'
import {
  MetricType,
  type ConnectorOutput,
  type ParsedPointPayload,
  type Timeserie,
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
  sourcePointId?: string
  mostRecentAvailableDate?: string | undefined
}

type DeclarationProcessingContext = {
  id: string
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

type DeclarationProcessingContextResponse = {
  success: boolean
  data: DeclarationProcessingContext
}

type LocalDeclarationFile = DeclarationFile & {
  localPath: string
}

type LegacySeriesValue = {
  date: string
  value: number
}

type LegacySeries = {
  pointPrelevement: string
  parameter: string
  unit: string | undefined
  frequency: string
  minDate: string
  maxDate: string
  data: LegacySeriesValue[]
}

type LegacyIngestionPayload = {
  series: LegacySeries[]
}

type IngestionResult = {
  errors: unknown[]
  data: LegacyIngestionPayload | undefined
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

function parseOptionalDate(value: string | undefined): Date | undefined {
  if (!value) {
    return undefined
  }

  const date = new Date(value)

  if (Number.isNaN(date.getTime())) {
    return undefined
  }

  return date
}

function metricTypeToLegacyParameter(metricType: MetricType): string {
  switch (metricType) {
    case MetricType.VOLUME_PRELEVE: {
      return 'volume prélevé'
    }

    case MetricType.INDEX: {
      return 'index'
    }
  }
}

function sanitizeFilename(filename: string): string {
  return path.basename(filename || 'file').replaceAll(/[^\w.-]+/g, '_')
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

  return point.pointId
}

function selectFileForConnector(parameters: {
  declarationType: string
  files: LocalDeclarationFile[]
}): LocalDeclarationFile | undefined {
  const {declarationType, files} = parameters

  switch (declarationType) {
    case 'template-file': {
      return files.find((file) => file.type === 'template-file') ?? files[0]
    }

    case 'aquasys':
    case 'extract-aquasys': {
      return (
        files.find((file) => file.type === 'aquasys') ??
        files.find((file) => file.type === 'extract-aquasys') ??
        files[0]
      )
    }

    default: {
      return undefined
    }
  }
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
      date: toIsoDate(value.date),
      value: value.value,
    }))

  if (values.length === 0) {
    return undefined
  }

  const sortedValues = [...values].sort((left, right) =>
    left.date.localeCompare(right.date),
  )

  return {
    pointPrelevement: point.name,
    parameter: metricTypeToLegacyParameter(metric.type),
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
  outputs: Array<{
    point: DeclarationPoint
    output: ConnectorOutput
  }>
}): LegacyIngestionPayload {
  const series: LegacySeries[] = []

  for (const {point, output} of parameters.outputs) {
    for (const metric of output.data.metrics) {
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

  return {series}
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

  const body = (await response.json()) as ServiceAccountTokenResponse
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

  const body = (await response.json()) as DeclarationProcessingContextResponse

  if (!body.success || !body.data) {
    throw new Error(
      '[process-declaration] Invalid declaration context response',
    )
  }

  if (!Array.isArray(body.data.files)) {
    throw new TypeError('[process-declaration] Invalid declaration files')
  }

  if (!Array.isArray(body.data.points)) {
    throw new TypeError('[process-declaration] Invalid declaration points')
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
      data: null,
    }
  }

  const connector = connectorRegistry.get(connectorName)

  if (!connector) {
    return {
      errors: [`Connecteur introuvable: ${connectorName}`],
      data: null,
    }
  }

  const sourceFile = selectFileForConnector({
    declarationType: declaration.type,
    files: localFiles,
  })

  if (!sourceFile) {
    return {
      errors: [
        `Aucun fichier exploitable pour la déclaration ${declaration.id}`,
      ],
      data: null,
    }
  }

  const outputs: Array<{
    point: DeclarationPoint
    output: ConnectorOutput
  }> = []

  for (const point of declaration.points) {
    const sourcePointId = resolveSourcePointId({
      connectorName,
      point,
    })

    try {
      const output = await connector.run({
        serviceAccount: 'declaration-upload',
        sourcePointId,
        sourceFile: sourceFile.localPath,
        // Pour une déclaration, on veut relire tout le fichier.
        // On ne veut pas filtrer à partir de la date d'activation du connecteur.
        mostRecentAvailableDate:
          parseOptionalDate(point.mostRecentAvailableDate) ??
          new Date('1900-01-01T00:00:00.000Z'),
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
        connector: connectorName,
        error: message,
      })
    }
  }

  const data = connectorOutputsToLegacyPayload({
    outputs,
  })

  return {
    errors,
    data: data.series.length > 0 ? data : null,
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
        data: parameters.result.data,
      }),
    },
  )

  if (!response.ok) {
    throw new Error(
      `[process-declaration] POST declaration ingest failed with status ${response.status}: ${await response.text()}`,
    )
  }
}

export async function processDeclaration(declarationId: string): Promise<void> {
  console.log(`[process-declaration] Processing declaration ${declarationId}`)

  const token = await getServiceAccountToken()
  const declaration = await getDeclarationProcessingContext(
    declarationId,
    token,
  )

  console.log(
    `[process-declaration] Declaration ${declaration.id}, type=${declaration.type}, files=${declaration.files.length}, points=${declaration.points.length}`,
  )

  const temporaryDirectory = await mkdtemp(
    path.join(tmpdir(), `ple-declaration-${declarationId}-`),
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
      declarationId,
      token,
      result,
    })

    console.log(
      `[process-declaration] Declaration ${declarationId} ingested: series=${result.data?.series.length ?? 0}, errors=${result.errors.length}`,
    )
  } finally {
    await rm(temporaryDirectory, {
      recursive: true,
      force: true,
    })
  }
}
