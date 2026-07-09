#!/usr/bin/env node
import process, {loadEnvFile} from 'node:process'
import {processDeclaration} from '../src/jobs/process-declaration.js'

type ReplayDeclarationCliOptions = {
  declarationIdentifier: string | undefined
  envFile: string | undefined
  pleBaseUrl: string | undefined
  clientId: string | undefined
  clientSecret: string | undefined
  help: boolean
}

function printUsage() {
  console.log(`Usage: npm run replay:declaration -- <code-ou-uuid> [options]

Options:
  --env-file <path>       Charge un fichier d'environnement avant le replay
  --ple-base-url <url>    Surcharge PLE_BASE_URL
  --client-id <value>     Surcharge CLIENT_ID
  --client-secret <value> Surcharge CLIENT_SECRET
  --help                  Affiche cette aide

Exemples:
  npm run replay:declaration -- TFPMDU --env-file .env
  npm run replay:declaration -- 6b071500-1535-4444-a275-7198740f9216 --ple-base-url http://127.0.0.1:5017
`)
}

function readOptionValue(
  cliArguments: string[],
  index: number,
  optionName: string,
) {
  const value = cliArguments[index + 1]

  if (!value || value.startsWith('--')) {
    throw new Error(`Option ${optionName} attend une valeur`)
  }

  return value
}

function parseArguments(cliArguments: string[]): ReplayDeclarationCliOptions {
  const options: ReplayDeclarationCliOptions = {
    declarationIdentifier: undefined,
    envFile: undefined,
    pleBaseUrl: undefined,
    clientId: undefined,
    clientSecret: undefined,
    help: false,
  }

  for (let index = 0; index < cliArguments.length; index += 1) {
    const argument = cliArguments[index]

    if (argument === '--help' || argument === '-h') {
      options.help = true
      continue
    }

    if (argument === '--env-file') {
      options.envFile = readOptionValue(cliArguments, index, argument)
      index += 1
      continue
    }

    if (argument === '--ple-base-url') {
      options.pleBaseUrl = readOptionValue(cliArguments, index, argument)
      index += 1
      continue
    }

    if (argument === '--client-id') {
      options.clientId = readOptionValue(cliArguments, index, argument)
      index += 1
      continue
    }

    if (argument === '--client-secret') {
      options.clientSecret = readOptionValue(cliArguments, index, argument)
      index += 1
      continue
    }

    if (argument.startsWith('--')) {
      throw new Error(`Option inconnue: ${argument}`)
    }

    if (options.declarationIdentifier) {
      throw new Error(
        `Un seul identifiant de déclaration est attendu: ${options.declarationIdentifier}, ${argument}`,
      )
    }

    options.declarationIdentifier = argument
  }

  return options
}

function setEnvOverride(name: string, value: string | undefined) {
  if (value) {
    process.env[name] = value
  }
}

async function main() {
  const options = parseArguments(process.argv.slice(2))

  if (options.help) {
    printUsage()
    return
  }

  if (!options.declarationIdentifier) {
    printUsage()
    throw new Error('Code ou UUID de déclaration requis')
  }

  if (options.envFile) {
    loadEnvFile(options.envFile)
  }

  setEnvOverride('PLE_BASE_URL', options.pleBaseUrl)
  setEnvOverride('CLIENT_ID', options.clientId)
  setEnvOverride('CLIENT_SECRET', options.clientSecret)

  const result = await processDeclaration(options.declarationIdentifier)

  console.log(
    `[replay-declaration] OK declaration=${result.declarationId}, code=${result.declarationCode ?? 'n/a'}, series=${result.seriesCount}, errors=${result.errorCount}`,
  )
}

try {
  await main()
} catch (error: unknown) {
  console.error(error instanceof Error ? error.message : error)
  process.exitCode = 1
}
