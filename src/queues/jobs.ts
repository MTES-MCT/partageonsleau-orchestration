import {getQueue} from './config.js'

const JOB_NAME = 'pull-updated-data'

export async function addJobPullUpdatedData(
  options: {
    trigger?: 'http' | 'scheduler' | 'manual'
  } = {},
) {
  const queue = getQueue(JOB_NAME)

  if (!queue) {
    console.log('[bullmq] Queue non disponible, job ignoré')
    return
  }

  const {trigger = 'manual'} = options

  return queue.add(
    JOB_NAME,
    {
      trigger,
    },
    {
      jobId: JOB_NAME,
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
