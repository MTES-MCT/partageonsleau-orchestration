import {isRivesEtEauxEnabled} from '../jobs/pull-rives-et-eaux.js'
import {JOBS, getQueue} from './config.js'

export async function startScheduler() {
  for (const job of JOBS) {
    if (!('cron' in job) || !job.cron) {
      continue
    }

    const queue = getQueue(job.name)

    if (!queue) {
      console.log(
        `[bullmq] Queue ${job.name} non disponible, planification ignorée`,
      )
      continue
    }

    if (job.name === 'pull-rives-et-eaux' && !isRivesEtEauxEnabled()) {
      await queue.removeJobScheduler(`${job.name}-daily`)
      continue
    }

    await queue.upsertJobScheduler(
      `${job.name}-daily`,
      {
        pattern: job.cron,
        ...('timeZone' in job ? {tz: job.timeZone} : {}),
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
