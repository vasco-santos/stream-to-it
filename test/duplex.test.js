const test = require('ava')
const { Duplex } = require('stream')
const pipe = require('it-pipe')
const { collect } = require('streaming-iterables')
const AbortController = require('abort-controller')
const abortable = require('abortable-iterator')
const Fifo = require('p-fifo')
const toIterable = require('../')
const { randomInt, randomBytes } = require('./helpers/random')

test('should convert to duplex iterable', async t => {
  const input = Array.from(Array(randomInt(5, 10)), () => randomBytes(1, 512))
  const fifo = new Fifo()

  const output = await pipe(
    input,
    toIterable.duplex(new Duplex({
      objectMode: true,
      write (chunk, enc, cb) {
        fifo.push(chunk).then(cb)
      },
      final (cb) {
        fifo.push(null).then(cb)
      },
      async read (size) {
        while (true) {
          const chunk = await fifo.shift()
          if (!this.push(chunk)) break
        }
      }
    })),
    collect
  )

  t.deepEqual(output, input)
})

test('should convert to abortable duplex iterable', async t => {
  const input = Array.from(Array(randomInt(5, 10)), () => randomBytes(1, 512))
  const fifo = new Fifo()

  const { sink, source } = toIterable.duplex(new Duplex({
    objectMode: true,
    write(chunk, enc, cb) {
      fifo.push(chunk).then(cb)
    },
    final(cb) {
      fifo.push(null).then(cb)
    },
    async read(size) {
      while (true) {
        const chunk = await fifo.shift()
        if (!this.push(chunk)) break
      }
    }
  }))

  // Create an abort signal and dial the socket
  const controller = new AbortController()
  const abortableIterator = {
    async sink (source) {
      source = abortable(source, controller.signal)

      try {
        await sink((async function * () {
          for await (const chunk of source) {
            // Convert BufferList to Buffer
            yield Buffer.isBuffer(chunk) ? chunk : chunk.slice()
          }
        })())
      } catch (err) {
        // If aborted we can safely ignore
        if (err.type !== 'aborted') {
          // If the source errored the socket will already have been destroyed by
          // toIterable.duplex(). If the socket errored it will already be
          // destroyed. There's nothing to do here except log the error & return.
          log.error(err)
        }
      }
    },
    source: abortable(source, controller.signal)
  }

  const output = await pipe(
    input,
    abortableIterator,
    collect
  )

  t.deepEqual(output, input)
})

test('should convert to abortable duplex iterable and abort while reading should throw abort error', async t => {
  const input = Array.from(Array(randomInt(5, 10)), () => randomBytes(1, 512))
  const fifo = new Fifo()

  const { sink, source } = toIterable.duplex(new Duplex({
    objectMode: true,
    write(chunk, enc, cb) {
      fifo.push(chunk).then(cb)
    },
    final(cb) {
      fifo.push(null).then(cb)
    },
    async read(size) {
      while (true) {
        const chunk = await fifo.shift()
        if (!this.push(chunk)) break
      }
    }
  }))

  // Create an abort signal and dial the socket
  const controller = new AbortController()

  async function* delayedResponse (source) {
    for await (const val of source) {
      await new Promise((resolve) => setTimeout(resolve, 1000))
      yield val
    }
  }

  const abortableIterator = {
    async sink(source) {
      source = abortable(delayedResponse(source), controller.signal)

      try {
        await sink((async function* () {
          for await (const chunk of source) {
            // Convert BufferList to Buffer
            yield Buffer.isBuffer(chunk) ? chunk : chunk.slice()
          }
        })())
      } catch (err) {
        // If aborted we can safely ignore
        if (err.type !== 'aborted') {
          // If the source errored the socket will already have been destroyed by
          // toIterable.duplex(). If the socket errored it will already be
          // destroyed. There's nothing to do here except log the error & return.
          log.error(err)
        }
      }
    },
    source: abortable(source, controller.signal)
  }

  try {
    setTimeout(() => controller.abort(), 100)

    await pipe(
      input,
      abortableIterator,
      collect
    )
  } catch (err) {
    t.deepEqual(err.code, 'ABORT_ERR')
    return
  }
  throw new Error('Did not throw an abort error')
})
