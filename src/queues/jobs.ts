import {getQueue} from './config.js'

const PULL_UPDATED_DATA_JOB_NAME = 'pull-updated-data'
const PROCESS_DECLARATION_JOB_NAME = 'process-declaration'

export async function addJobPullUpdatedData(
  options: {
    trigger?: 'http' | 'scheduler' | 'manual'
  } = {},
) {
  const queue = getQueue(PULL_UPDATED_DATA_JOB_NAME)

  const {trigger = 'manual'} = options

  return queue.add(
    PULL_UPDATED_DATA_JOB_NAME,
    {
      trigger,
    },
    {
      jobId: PULL_UPDATED_DATA_JOB_NAME,
      removeOnComplete: true,
      removeOnFail: false,
      attempts: 3,
      backoff: {
        type: 'exponential',
        delay: 5000,
      },
    },
  )
}

export async function addJobProcessDeclaration(parameters: {
  declarationId: string
}) {
  const queue = getQueue(PROCESS_DECLARATION_JOB_NAME)

  return queue.add(
    PROCESS_DECLARATION_JOB_NAME,
    {
      declarationId: parameters.declarationId,
    },
    {
      jobId: `declaration-${parameters.declarationId}`,
      removeOnComplete: true,
      removeOnFail: false,
      attempts: 3,
      backoff: {
        type: 'exponential',
        delay: 5000,
      },
    },
  )
}
