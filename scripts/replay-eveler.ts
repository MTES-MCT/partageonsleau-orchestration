#!/usr/bin/env node
import {loadEnvFile} from 'node:process'
import {parseArgs} from 'node:util'
import {replayEveler} from '../src/jobs/replay-eveler.js'

try {
  const {values} = parseArgs({
    options: {
      connector: {type: 'string'},
      declarant: {type: 'string'},
      start: {type: 'string'},
      end: {type: 'string'},
      'env-file': {type: 'string'},
      'cache-dir': {type: 'string'},
      'cache-only': {type: 'boolean', default: false},
      apply: {type: 'boolean', default: false},
      help: {type: 'boolean', short: 'h'},
    },
  })
  if (values.help) {
    console.log(
      'npm run replay:eveler -- --connector UUID --declarant UUID --start YYYY-MM-DDTHH:00:00Z --end YYYY-MM-DDTHH:00:00Z [--env-file PATH] [--cache-dir PRIVATE_PATH] [--cache-only] [--apply]\nDry-run by default: reads PLE context and provider/cache, never ingests. End is exclusive. Only full UTC hours are accepted. Cache contains private source data; keep it outside Git. Reuse identical bounds with --cache-only in another environment to avoid provider calls.',
    )
  } else {
    if (values['env-file']) loadEnvFile(values['env-file'])
    if (
      !values.connector ||
      !values.declarant ||
      !values.start ||
      !values.end ||
      !values.start.endsWith('Z') ||
      !values.end.endsWith('Z')
    ) {
      throw new Error(
        'Required: --connector, --declarant, --start and --end (explicit UTC timestamps).',
      )
    }
    const report = await replayEveler({
      connectorId: values.connector,
      declarantId: values.declarant,
      window: {start: new Date(values.start), end: new Date(values.end)},
      apply: values.apply,
      cacheDirectory: values['cache-dir'],
      cacheOnly: values['cache-only'],
    })
    console.log(`[replay-eveler] ${JSON.stringify(report)}`)
  }
} catch (error) {
  // PLE authentication/context errors can include remote response bodies.
  const safe =
    error instanceof Error &&
    /^\[(?:eveler|replay-eveler)\]/v.test(error.message)
  console.error(
    safe
      ? error.message
      : '[replay-eveler] Replay failed; check target configuration and provider/API availability. No success acknowledged.',
  )
  process.exitCode = 1
}
