import {JOBS, getQueue} from './config.js'

export async function startScheduler() {
  for (const job of JOBS) {
    if (!job.cron) {
      continue
    }

    const queue = getQueue(job.name)

    if (!queue) {
      console.log(
        `[bullmq] Queue ${job.name} non disponible, planification ignorée`,
      )
      continue
    }

    await queue.upsertJobScheduler(
      `${job.name}-daily`,
      {
        pattern: job.cron,
      },
      {
        name: job.name,
        data: {
          trigger: 'scheduler',
        },
        opts: {
          removeOnComplete: true,
          removeOnFail: false,
          attempts: 3,
          backoff: {
            type: 'exponential',
            delay: 5000,
          },
        },
      },
    )
  }
}
