#!/usr/bin/env node
import {loadEnvFile} from 'node:process'
import {parseArgs} from 'node:util'
import {randomUUID} from 'node:crypto'
import {lstat, mkdir, open, readFile, realpath, rename} from 'node:fs/promises'
import path from 'node:path'
import {
  applyWillieReplay,
  prepareWillieReplay,
} from '../src/jobs/replay-willie.js'

async function privatePath(filename: string): Promise<void> {
  if (!path.isAbsolute(filename))
    throw new Error('WILLIE_REPLAY_PRIVATE_PATH_REQUIRED')
  await mkdir(path.dirname(filename), {recursive: true, mode: 0o700})
  let directory = await realpath(path.dirname(filename))
  for (;;) {
    try {
      await lstat(path.join(directory, '.git'))
      throw new Error('WILLIE_REPLAY_REPORT_INSIDE_GIT')
    } catch (error) {
      if (
        !(error instanceof Error) ||
        !('code' in error) ||
        error.code !== 'ENOENT'
      )
        throw error
    }
    const parent = path.dirname(directory)
    if (parent === directory) break
    directory = parent
  }
}

async function save(
  filename: string,
  value: unknown,
  replace = false,
): Promise<void> {
  await privatePath(filename)
  const destination = replace ? `${filename}.${randomUUID()}.tmp` : filename
  const file = await open(destination, 'wx', 0o600)
  try {
    await file.writeFile(JSON.stringify(value, null, 2) + '\n')
    await file.sync()
  } finally {
    await file.close()
  }
  if (replace) await rename(destination, filename)
  const directory = await open(path.dirname(filename), 'r')
  try {
    await directory.sync()
  } finally {
    await directory.close()
  }
}

async function main() {
  process.umask(0o077)
  const {values} = parseArgs({
    options: {
      target: {type: 'string'},
      connector: {type: 'string'},
      declarant: {type: 'string'},
      start: {type: 'string'},
      end: {type: 'string'},
      'env-file': {type: 'string'},
      'cache-dir': {type: 'string'},
      report: {type: 'string'},
      'against-plan': {type: 'string'},
      'plan-hash': {type: 'string'},
      apply: {type: 'boolean', default: false},
      help: {type: 'boolean', short: 'h'},
    },
  })
  if (values.help) {
    console.log(
      'Dry-run: --target prod|testing --connector UUID --declarant UUID --start YYYY-MM-DD --end YYYY-MM-DD --cache-dir PRIVATE --report PRIVATE --env-file PRIVATE\nApply: --apply --target prod|testing --against-plan PRIVATE --plan-hash SHA256 --cache-dir PRIVATE --report PRIVATE --env-file PRIVATE\nCached responses only. Complete provider calendar days; end exclusive. Payload periods preserve legacy UTC midnight labels. No provider requests, queues or workers.',
    )
    return
  }
  if (
    (values.target !== 'prod' && values.target !== 'testing') ||
    !values['cache-dir'] ||
    !values.report ||
    !values['env-file']
  ) {
    throw new Error('WILLIE_REPLAY_ARGUMENTS_INVALID')
  }
  await privatePath(values.report)
  const inputPaths = [values['against-plan'], values['env-file']]
    .filter((value): value is string => Boolean(value))
    .map((value) => path.resolve(value))
  if (inputPaths.includes(path.resolve(values.report)))
    throw new Error('WILLIE_REPLAY_OUTPUT_OVERWRITES_INPUT')
  loadEnvFile(values['env-file'])
  if (values.apply) {
    if (
      !values['against-plan'] ||
      !values['plan-hash'] ||
      [values.connector, values.declarant, values.start, values.end].some(
        Boolean,
      )
    ) {
      throw new Error('WILLIE_REPLAY_APPLY_ARGUMENTS_INVALID')
    }
    const plan: unknown = JSON.parse(
      await readFile(values['against-plan'], 'utf8'),
    )
    // An uncertain network result keeps a durable PREPARED receipt. Reconciliation
    // against the frozen plan is required before claiming that a retry succeeded.
    await save(values.report, {
      status: 'PREPARED',
      planHash: values['plan-hash'],
      target: values.target,
      preparedAt: new Date().toISOString(),
    })
    const receipt = await applyWillieReplay({
      target: values.target,
      plan,
      expectedPlanHash: values['plan-hash'],
      cacheDirectory: values['cache-dir'],
    })
    await save(values.report, {...receipt, status: 'ACKNOWLEDGED'}, true)
    console.log(
      JSON.stringify({
        mode: 'apply',
        target: values.target,
        planHash: receipt.planHash,
        numericDays: receipt.numericDays,
        acknowledgement: receipt.acknowledgement,
        report: values.report,
      }),
    )
    return
  }
  if (
    !values.connector ||
    !values.declarant ||
    !values.start ||
    !values.end ||
    values['against-plan'] ||
    values['plan-hash']
  ) {
    throw new Error('WILLIE_REPLAY_PREPARE_ARGUMENTS_INVALID')
  }
  const plan = await prepareWillieReplay({
    target: values.target,
    connectorId: values.connector,
    declarantId: values.declarant,
    window: {start: values.start, end: values.end},
    cacheDirectory: values['cache-dir'],
  })
  await save(values.report, plan)
  console.log(
    JSON.stringify({
      mode: 'dry-run',
      target: values.target,
      planHash: plan.planHash,
      summary: plan.summary,
      report: values.report,
    }),
  )
}

main().catch((error: unknown) => {
  const code =
    error instanceof Error && /^WILLIE_REPLAY_[A-Z_]+$/v.test(error.message)
      ? error.message
      : 'WILLIE_REPLAY_FAILED'
  console.error(JSON.stringify({error: code}))
  process.exitCode = 1
})
