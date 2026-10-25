import {readFile, writeFile} from 'node:fs/promises'
import path from 'node:path'
import process from 'node:process'
import {parseArgs} from 'node:util'
import {parseReunionFile} from '../src/connectors/reunion.js'
import {ConflictPolicy} from '../src/connectors/types.js'

const {values} = parseArgs({
  options: {
    file: {type: 'string'},
    type: {type: 'string'},
    'file-id': {type: 'string'},
    output: {type: 'string'},
  },
})
if (!values.file || !values.type || !values.output) {
  console.error(
    'Usage: parse-reunion --file <classeur> --type <type> [--file-id <id>] --output <fichier-json-privé>',
  )
  process.exitCode = 1
} else {
  const result = parseReunionFile({
    buffer: await readFile(values.file),
    filename: path.basename(values.file),
    fileId: values['file-id'],
    typeCode: values.type,
  })
  await writeFile(
    values.output,
    JSON.stringify(
      {
        errors: result.errors,
        data: {
          nativeTemporal: true,
          conflictPolicy: ConflictPolicy.SKIP_NEW_CHUNK,
          series: result.series,
        },
      },
      null,
      2,
    ),
    {mode: 0o600},
  )
  console.log(
    JSON.stringify({
      series: result.series.length,
      values: result.series.reduce(
        (sum, series) => sum + series.data.length,
        0,
      ),
      issues: result.errors.length,
    }),
  )
}
