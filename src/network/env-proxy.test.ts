import assert from 'node:assert/strict'
import {execFile} from 'node:child_process'
import {createServer, type Server} from 'node:http'
import process from 'node:process'
import test, {type TestContext} from 'node:test'

async function listen(t: TestContext, server: Server): Promise<number> {
  await new Promise<void>((resolve) => {
    server.listen(0, '127.0.0.1', resolve)
  })
  t.after(() => {
    server.closeAllConnections()
    server.close()
  })
  const address = server.address()
  assert.ok(address && typeof address !== 'string')
  return address.port
}

async function fetchInChild(
  url: string,
  proxy: string,
  noProxy = '',
  resolveToLoopback = false,
) {
  // Node reads proxy configuration at startup. Lower-case variables take
  // precedence, including empty values inherited from the test runner.
  const environment = Object.fromEntries(
    Object.entries(process.env).filter(
      ([name]) =>
        !/^(?:https?_proxy|all_proxy|no_proxy|node_options|node_use_env_proxy)$/iv.test(
          name,
        ),
    ),
  )
  const stdout = await new Promise<string>((resolve, reject) => {
    execFile(
      process.execPath,
      [
        '--dns-result-order=ipv4first',
        '--input-type=module',
        '--eval',
        `
          import dns from 'node:dns';
          if (process.argv[2] === 'loopback') {
            dns.lookup = (_hostname, options, callback) => {
              const done = typeof options === 'function' ? options : callback;
              if (options?.all) done(null, [{address: '127.0.0.1', family: 4}]);
              else done(null, '127.0.0.1', 4);
            };
          }
          const response = await fetch(process.argv[1], {signal: AbortSignal.timeout(3000)});
          console.log(await response.text());
        `,
        url,
        resolveToLoopback ? 'loopback' : '',
      ],
      {
        env: {
          ...environment,
          NODE_USE_ENV_PROXY: '1',
          HTTP_PROXY: proxy,
          HTTPS_PROXY: proxy,
          NO_PROXY: noProxy,
        },
        timeout: 5000,
      },
      (error, stdout) => {
        if (error) {
          reject(new Error('Child fetch failed', {cause: error}))
          return
        }

        resolve(stdout)
      },
    )
  })
  return stdout.trim()
}

async function fakeProxy(t: TestContext, deny = false) {
  const requests: string[] = []
  const server = createServer((_request, response) => {
    response.writeHead(500).end()
  })
  server.on('connect', (request, socket) => {
    requests.push(request.url ?? '')
    if (deny) {
      socket.end('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n')
      return
    }

    socket.write('HTTP/1.1 200 Connection Established\r\n\r\n')
    socket.once('data', () => {
      socket.end(
        'HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: close\r\n\r\nproxied',
      )
    })
    t.after(() => socket.destroy())
  })
  const port = await listen(t, server)
  return {url: `http://127.0.0.1:${port}`, requests}
}

void test('native fetch uses the environment proxy without a per-request agent', async (t) => {
  const proxy = await fakeProxy(t)
  assert.equal(
    await fetchInChild('http://provider.invalid/data', proxy.url),
    'proxied',
  )
  assert.deepEqual(proxy.requests, ['provider.invalid:80'])
})

void test('HTTPS fetch uses HTTPS_PROXY and rejects a denied CONNECT', async (t) => {
  const proxy = await fakeProxy(t, true)
  await assert.rejects(fetchInChild('https://provider.invalid/data', proxy.url))
  assert.deepEqual(proxy.requests, ['provider.invalid:443'])
})

void test('NO_PROXY bypasses a configured host but not a lookalike', async (t) => {
  let directRequests = 0
  const origin = createServer((_request, response) => {
    directRequests++
    response.end('direct')
  })
  const port = await listen(t, origin)
  const proxy = await fakeProxy(t)
  assert.equal(
    await fetchInChild(`http://localhost:${port}/data`, proxy.url, 'localhost'),
    'direct',
  )
  assert.equal(
    await fetchInChild(
      'http://localhost.provider.invalid/data',
      proxy.url,
      'localhost',
    ),
    'proxied',
  )
  assert.equal(directRequests, 1)
  assert.deepEqual(proxy.requests, ['localhost.provider.invalid:80'])
})

void test('NO_PROXY also bypasses subdomains of a configured host', async (t) => {
  const origin = createServer((_request, response) => {
    response.end('direct')
  })
  const port = await listen(t, origin)
  const proxy = await fakeProxy(t)
  assert.equal(
    await fetchInChild(
      `http://child.service.invalid:${port}/data`,
      proxy.url,
      'service.invalid',
      true,
    ),
    'direct',
  )
  assert.deepEqual(proxy.requests, [])
})

void test('a refused proxy never falls back to a reachable direct origin', async (t) => {
  let directRequests = 0
  const origin = createServer((_request, response) => {
    directRequests++
    response.end('direct')
  })
  const port = await listen(t, origin)
  const proxy = await fakeProxy(t, true)
  await assert.rejects(fetchInChild(`http://localhost:${port}/data`, proxy.url))
  assert.equal(directRequests, 0)
  assert.deepEqual(proxy.requests, [`localhost:${port}`])
})
