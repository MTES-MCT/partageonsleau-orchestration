import assert from 'node:assert/strict'
import {Buffer} from 'node:buffer'
import {once} from 'node:events'
import test from 'node:test'
import express from 'express'
import {createBullBoardRouter} from './board.js'

void test('Bull Board reste protégé et sert son interface et son API après migration', async (t) => {
  const {router, close} = createBullBoardRouter(
    '/admin/queues',
    'synthetic-board-password',
    {queues: []},
  )
  const app = express()
  app.use('/admin/queues', router)
  const server = app.listen(0, '127.0.0.1')
  t.after(async () => {
    server.closeAllConnections()
    await new Promise<void>((resolve, reject) => {
      server.close((error) => {
        if (error) {
          reject(error)
        } else {
          resolve()
        }
      })
    })
    await close()
  })
  await once(server, 'listening')
  const address = server.address()
  if (address === null || typeof address === 'string') {
    throw new Error('Expected the synthetic dashboard server to listen on TCP')
  }

  const url = `http://127.0.0.1:${address.port}/admin/queues`
  const unauthenticated = await fetch(url)
  assert.equal(unauthenticated.status, 401)
  assert.match(unauthenticated.headers.get('www-authenticate') ?? '', /Basic/v)
  const invalid = await fetch(url, {headers: {Authorization: 'Basic !!!'}})
  assert.equal(invalid.status, 401)

  const headers = {
    Authorization: `Basic ${Buffer.from('admin:synthetic-board-password').toString('base64')}`,
  }
  const page = await fetch(url, {headers})
  assert.equal(page.status, 200)
  assert.match(page.headers.get('content-type') ?? '', /text\/html/v)
  assert.match(await page.text(), /<html/v)

  const queues = await fetch(`${url}/api/queues`, {headers})
  assert.equal(queues.status, 200)
  assert.match(queues.headers.get('content-type') ?? '', /application\/json/v)
  const payload = (await queues.json()) as {queues: unknown[]}
  assert.deepEqual(payload.queues, [])
})
