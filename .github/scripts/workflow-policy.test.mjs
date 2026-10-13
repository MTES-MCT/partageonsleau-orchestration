import assert from 'node:assert/strict'
import {spawnSync} from 'node:child_process'
import {mkdtempSync, readFileSync, rmSync} from 'node:fs'
import {tmpdir} from 'node:os'
import {join} from 'node:path'
import {fileURLToPath} from 'node:url'
import test from 'node:test'

const root = fileURLToPath(new URL('../../', import.meta.url))
const imageOutput = '${{ needs.image.outputs.image }}'
const repository = 'rg.fr-par.scw.cloud/partageonsleau-orchestration/partageonsleau-orchestration'
const requiredGate = "${{ success() && needs.quality.result == 'success' && needs.image.result == 'success' }}"

// These are contracts for the repository's block-style jobs and steps. Actionlint
// separately validates the full YAML and GitHub Actions expression syntax.
function sections(text, pattern) {
  const matches = [...text.matchAll(pattern)]
  return Object.fromEntries(matches.map((match, index) => [
    match[1], text.slice(match.index, matches[index + 1]?.index ?? text.length)
  ]))
}

function jobs(text) {
  return sections(text.slice(text.indexOf('\njobs:\n')), /^ {2}([a-z_]+):\n/gm)
}

function steps(job) {
  return sections(job, /^ {6}- name: (.+)\n/gm)
}

function assertTopology(text, environment) {
  const graph = jobs(text)
  assert.deepEqual(Object.keys(graph), ['quality', 'image', 'deploy'])
  assert.match(text, /cancel-in-progress: false/)
  assert.match(graph.quality, /uses: \.\/\.github\/workflows\/quality\.yml/)
  assert.doesNotMatch(graph.image, /^ {4}needs:/m)
  assert.match(graph.deploy, /^ {4}needs: \[quality, image\]$/m)
  assert.ok(graph.deploy.includes('    if: ' + requiredGate + '\n'))
  assert.doesNotMatch(graph.image + graph.deploy, /continue-on-error:/)
  assert.doesNotMatch(graph.image, /run: npm ci|npm install --global|container.*update|imagetools create/)
  assert.doesNotMatch(graph.deploy, /docker\/build-push-action|steps\.build\.outputs/)
  assert.ok(graph.image.includes('image: ${{ steps.candidate.outputs.image }}'))
  assert.ok(graph.image.includes('scope=orchestration-' + environment))
  assert.ok(graph.image.includes(':' + environment + '-${{ github.sha }}'))

  const imageSteps = steps(graph.image)
  const deploymentSteps = steps(graph.deploy)
  const scan = imageSteps['Scan exact candidate image before any deployment']
  const candidate = imageSteps['Export verified immutable image']
  assert.ok(scan?.includes('scan-image.sh "$IMAGE_REF"'))
  assert.ok(scan?.includes('smoke-image.sh "$IMAGE_REF"'))
  assert.doesNotMatch(scan + candidate, /^ {8}if:/m)
  assert.ok(graph.image.indexOf(scan) < graph.image.indexOf(candidate))
  assert.ok(candidate.includes('assertImmutableImage(process.env.IMAGE_REF, repository)'))
  assert.ok(candidate.indexOf('assertImmutableImage(process.env') < candidate.indexOf('appendFileSync(process.env.GITHUB_OUTPUT'))

  for (const job of [graph.image, graph.deploy]) {
    assert.ok(job.includes('deploy-container.mjs --check ' + environment))
    assert.match(job, /node-version-file: "?\.nvmrc"?/)
  }
  assert.match(graph.deploy, /run: npm ci/)
  assert.ok(graph.deploy.includes('Validate Scaleway ' + environment + ' resource ownership'))
  assert.ok(graph.deploy.includes('deploy/certs/' + environment + '/redis-ca.pem'))
  const entries = Object.entries(deploymentSteps)
  const mutation = entries.findIndex(([, step]) => step.includes('node .github/scripts/deploy-container.mjs "$SCW_CONTAINER_ID" /health'))
  const alias = entries.findIndex(([, step]) => step.includes('imagetools create'))
  assert.ok(mutation >= 0 && alias > mutation)
  for (const index of [mutation, alias]) {
    assert.ok(entries[index][1].includes('IMAGE_REF: ' + imageOutput))
    assert.doesNotMatch(entries[index][1], /^ {8}if:/m)
  }
  if (environment === 'prod') {
    assert.equal(entries[mutation - 1][0], 'Require idle orchestration queues before deployment')
    assert.ok(entries[mutation - 1][1].includes('assertRemoteOrchestrationIdle'))
    assert.doesNotMatch(graph.image, /assertRemoteOrchestrationIdle/)
  }
  return graph
}

for (const environment of ['testing', 'demo', 'prod']) {
  const filename = environment === 'prod' ? 'deploy.yml' : `deploy-${environment}.yml`
  const source = readFileSync(join(root, '.github/workflows', filename), 'utf8')

  test(`${environment}: parallel image is isolated; deployment requires quality, scan and immutable output`, () => {
    assertTopology(source, environment)
  })

  test(`${environment}: refuses missing dependencies, failed-job bypass and mutable image wiring`, () => {
    for (const [before, after] of [
      ['needs: [quality, image]', 'needs: image'],
      [requiredGate, '${{ always() }}'],
      ['    timeout-minutes: 30', '    continue-on-error: true\n    timeout-minutes: 30'],
      ['bash .github/scripts/scan-image.sh "$IMAGE_REF"', 'true'],
      ['bash .github/scripts/smoke-image.sh "$IMAGE_REF"', 'true'],
      [imageOutput, '${{ env.SCW_REGISTRY_ENDPOINT }}/${{ env.SCW_IMAGE_NAME }}:latest']
    ]) {
      assert.ok(source.includes(before))
      assert.throws(() => assertTopology(source.replace(before, after), environment))
    }
  })

  test(`${environment}: actual output step exports only a valid digest in its configured repository`, () => {
    const candidate = steps(jobs(source).image)['Export verified immutable image']
    const script = candidate.split("<<'NODE'\n")[1].split('\n          NODE')[0]
    const directory = mkdtempSync(join(tmpdir(), 'orchestration-image-output-'))
    try {
      for (const [index, image] of [
        repository + '@sha256:' + 'a'.repeat(64), '', repository + ':latest',
        repository + '@sha256:invalid', 'rg.fr-par.scw.cloud/another/image@sha256:' + 'a'.repeat(64)
      ].entries()) {
        const output = join(directory, String(index))
        const result = spawnSync(process.execPath, ['--input-type=module'], {
          cwd: root, input: script, encoding: 'utf8',
          env: {...process.env, IMAGE_REF: image, GITHUB_OUTPUT: output,
            SCW_REGISTRY_ENDPOINT: 'rg.fr-par.scw.cloud/partageonsleau-orchestration',
            SCW_IMAGE_NAME: 'partageonsleau-orchestration'}
        })
        if (index === 0) {
          assert.equal(result.status, 0, result.stderr)
          assert.equal(readFileSync(output, 'utf8'), 'image=' + image + '\n')
        } else {
          assert.notEqual(result.status, 0)
          assert.throws(() => readFileSync(output), {code: 'ENOENT'})
        }
      }
    } finally {
      rmSync(directory, {recursive: true, force: true})
    }
  })
}

test('prod: moving the idle check before intervening setup steps is rejected', () => {
  const source = readFileSync(join(root, '.github/workflows/deploy.yml'), 'utf8')
  const altered = source.replace('      - name: Deploy and verify production container',
    '      - name: Intervening setup\n        run: echo setup\n\n      - name: Deploy and verify production container')
  assert.throws(() => assertTopology(altered, 'prod'))
})
