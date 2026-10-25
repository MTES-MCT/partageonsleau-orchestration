/** Local rehearsals can consume declaration uploads without any provider job. */
export function selectedWorkerNames(
  names: string[],
  environment = process.env,
): string[] {
  const selection = environment.WORKER_JOBS?.split(',')
    .map((name) => name.trim())
    .filter(Boolean)
  if (!selection) return names
  const unknown = selection.filter((name) => !names.includes(name))
  if (unknown.length)
    throw new Error(`Unknown WORKER_JOBS: ${unknown.join(', ')}`)
  return names.filter((name) => selection.includes(name))
}

export function isSchedulerEnabled(environment = process.env): boolean {
  return environment.SCHEDULER_ENABLED !== 'false'
}
