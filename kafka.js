// MSK check: connects with whatever the msk dependency exported, keeps a
// consumer on one topic, and exposes a page to publish and watch messages.
//
// Provisioned clusters export SCRAM credentials. Serverless clusters export no
// credentials at all: the pod's IRSA role signs an OAUTHBEARER token instead.
const os = require('os')
const { Kafka, logLevel } = require('kafkajs')
const { generateAuthToken } = require('aws-msk-iam-sasl-signer-js')

const TOPIC = process.env.KAFKA_TOPIC || 'lops-msk-check'
// One group per pod, so every replica sees every message and a round trip
// is always consumed by the pod that published it.
const GROUP = `${process.env.KAFKA_GROUP || 'lops-msk-check'}-${os.hostname()}`
const KEEP = 50

const env = {
  brokers: (process.env.KAFKA_BROKERS || '').split(',').map((b) => b.trim()).filter(Boolean),
  mechanism: process.env.KAFKA_SASL_MECHANISM || '',
  protocol: process.env.KAFKA_SECURITY_PROTOCOL || '',
  region: process.env.KAFKA_REGION || process.env.AWS_REGION || '',
  username: process.env.KAFKA_USERNAME || '',
  password: process.env.KAFKA_PASSWORD || '',
}

// Serverless exports OAUTHBEARER; AWS_MSK_IAM is the name other MSK clients use.
const iam = ['OAUTHBEARER', 'AWS_MSK_IAM'].includes(env.mechanism)

const state = {
  configured: env.brokers.length > 0,
  consumer: 'not started',
  lastError: null,
  received: [],
  waiters: new Map(),
}

function sasl() {
  if (env.mechanism === 'SCRAM-SHA-512') {
    return { mechanism: 'scram-sha-512', username: env.username, password: env.password }
  }
  if (iam) {
    return {
      mechanism: 'oauthbearer',
      oauthBearerProvider: async () => {
        const { token } = await generateAuthToken({ region: env.region })
        return { value: token }
      },
    }
  }
  throw new Error(`unsupported KAFKA_SASL_MECHANISM ${JSON.stringify(env.mechanism)}`)
}

let kafka, producer, admin

function client() {
  if (!kafka) {
    kafka = new Kafka({
      clientId: 'sample-service',
      brokers: env.brokers,
      ssl: true,
      sasl: sasl(),
      logLevel: logLevel.WARN,
      connectionTimeout: 10000,
      authenticationTimeout: 10000,
    })
  }
  return kafka
}

async function getAdmin() {
  if (!admin) {
    const a = client().admin()
    await a.connect()
    admin = a
  }
  return admin
}

async function getProducer() {
  if (!producer) {
    const p = client().producer({ allowAutoTopicCreation: false })
    await p.connect()
    producer = p
  }
  return producer
}

async function ensureTopic() {
  const a = await getAdmin()
  const topics = await a.listTopics()
  if (!topics.includes(TOPIC)) {
    // -1 takes the broker's default replication; serverless only accepts that.
    await a.createTopics({ topics: [{ topic: TOPIC, numPartitions: 3, replicationFactor: -1 }], waitForLeaders: true })
  }
}

async function startConsumer() {
  if (!state.configured) {
    state.consumer = 'KAFKA_BROKERS unset, no msk dependency'
    return
  }
  try {
    state.consumer = 'connecting'
    await ensureTopic()
    const consumer = client().consumer({ groupId: GROUP })
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: false })
    await consumer.run({
      eachMessage: async ({ partition, message }) => {
        const value = message.value ? message.value.toString() : ''
        state.received.unshift({
          at: new Date().toISOString(),
          partition,
          offset: message.offset,
          key: message.key ? message.key.toString() : null,
          value,
        })
        state.received.length = Math.min(state.received.length, KEEP)
        const waiter = state.waiters.get(message.key && message.key.toString())
        if (waiter) waiter(partition, message.offset)
      },
    })
    state.consumer = 'running'
    state.lastError = null
  } catch (err) {
    state.consumer = 'failed'
    state.lastError = err.message
    console.error('kafka consumer failed:', err)
    setTimeout(startConsumer, 15000)
  }
}

async function publish(value, key) {
  const p = await getProducer()
  const [meta] = await p.send({ topic: TOPIC, messages: [{ key, value }] })
  return { partition: meta.partition, offset: meta.baseOffset }
}

async function status() {
  const a = await getAdmin()
  const cluster = await a.describeCluster()
  const [topic] = (await a.fetchTopicMetadata({ topics: [TOPIC] })).topics
  const offsets = await a.fetchTopicOffsets(TOPIC)
  return {
    brokers: cluster.brokers.map((b) => `${b.host}:${b.port}`),
    controller: cluster.controller,
    topic: TOPIC,
    partitions: topic.partitions.length,
    messagesInTopic: offsets.reduce((n, o) => n + (Number(o.high) - Number(o.low)), 0),
  }
}

// Publishes a uniquely keyed message and waits for our own consumer to see it.
async function roundtrip(timeoutMs = 20000) {
  const key = `roundtrip-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
  const started = Date.now()
  const seen = new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      state.waiters.delete(key)
      reject(new Error(`published but not consumed within ${timeoutMs / 1000}s (consumer: ${state.consumer})`))
    }, timeoutMs)
    state.waiters.set(key, (partition, offset) => {
      clearTimeout(timer)
      state.waiters.delete(key)
      resolve({ partition, offset })
    })
  })
  const sent = await publish(`roundtrip check at ${new Date().toISOString()}`, key)
  const got = await seen
  return { key, published: sent, consumed: got, ms: Date.now() - started }
}

const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]))

function page(st, flash) {
  const row = (k, v) => `<tr><td style="padding:6px 10px;font-weight:bold">${esc(k)}</td><td style="padding:6px 10px">${esc(v)}</td></tr>`
  const msgs = state.received.map((m) =>
    `<tr><td style="padding:4px 8px">${esc(m.at)}</td><td style="padding:4px 8px">${m.partition}/${esc(m.offset)}</td><td style="padding:4px 8px">${esc(m.key || '')}</td><td style="padding:4px 8px">${esc(m.value)}</td></tr>`).join('')
  return `<!doctype html><html><head><meta charset="utf-8"><title>MSK check</title>
<meta http-equiv="refresh" content="10"></head>
<body style="font-family:sans-serif;max-width:900px;margin:30px auto;padding:0 16px">
<h1>MSK check</h1>
${flash ? `<p style="padding:10px;background:${flash.ok ? '#e6f7ea' : '#fdecea'}">${esc(flash.text)}</p>` : ''}
<table style="border-collapse:collapse">
${row('Mechanism', env.mechanism || '(unset)')}
${row('Security protocol', env.protocol || '(unset)')}
${row('Bootstrap brokers', env.brokers.join(', ') || '(unset)')}
${row('Region', env.region || '(unset)')}
${row('Username', iam ? 'n/a (IAM)' : env.username || '(unset)')}
${row('Password', env.password ? 'set' : (iam ? 'n/a (IAM)' : '(unset)'))}
${row('Consumer', state.consumer + (state.lastError ? ` (${state.lastError})` : ''))}
${st.error ? row('Cluster', 'unreachable: ' + st.error) : `
${row('Cluster brokers', st.brokers.join(', '))}
${row('Topic', `${st.topic}, ${st.partitions} partitions, ${st.messagesInTopic} messages retained`)}`}
</table>
<form method="post" action="/kafka/publish" style="margin:20px 0">
  <input name="message" placeholder="message" style="width:60%;padding:6px" required>
  <button type="submit" style="padding:6px 12px">Publish</button>
</form>
<form method="post" action="/kafka/roundtrip" style="margin:0 0 20px">
  <button type="submit" style="padding:6px 12px">Run publish → consume round trip</button>
</form>
<h2>Last ${KEEP} consumed messages <small style="font-weight:normal;color:#888">(page refreshes every 10s)</small></h2>
<table style="border-collapse:collapse;width:100%">
<tr><th align="left">Received</th><th align="left">Partition/offset</th><th align="left">Key</th><th align="left">Value</th></tr>
${msgs || '<tr><td colspan="4" style="padding:8px;color:#888">nothing consumed yet</td></tr>'}
</table>
<p style="color:#888">JSON: <a href="/kafka/status">/kafka/status</a> · <a href="/kafka/messages">/kafka/messages</a> · POST /kafka/roundtrip</p>
</body></html>`
}

async function safeStatus() {
  if (!state.configured) return { error: 'KAFKA_BROKERS unset, no msk dependency' }
  try {
    return await status()
  } catch (err) {
    return { error: err.message }
  }
}

function routes(app) {
  const express = require('express')
  app.use('/kafka', express.urlencoded({ extended: false }), express.json())

  app.get('/kafka', async (req, res) => {
    const flash = req.query.ok ? { ok: true, text: req.query.ok } : req.query.err ? { ok: false, text: req.query.err } : null
    res.send(page(await safeStatus(), flash))
  })

  app.get('/kafka/status', async (req, res) => {
    const st = await safeStatus()
    res.status(st.error ? 503 : 200).json({ mechanism: env.mechanism, consumer: state.consumer, consumerError: state.lastError, ...st })
  })

  app.get('/kafka/messages', (req, res) => res.json(state.received))

  app.post('/kafka/publish', async (req, res) => {
    const message = (req.body && req.body.message) || `hello at ${new Date().toISOString()}`
    try {
      const r = await publish(message, `manual-${Date.now()}`)
      if (req.is('json')) return res.json(r)
      res.redirect(303, `/kafka?ok=${encodeURIComponent(`published to partition ${r.partition} at offset ${r.offset}`)}`)
    } catch (err) {
      if (req.is('json')) return res.status(500).json({ error: err.message })
      res.redirect(303, `/kafka?err=${encodeURIComponent('publish failed: ' + err.message)}`)
    }
  })

  app.post('/kafka/roundtrip', async (req, res) => {
    try {
      const r = await roundtrip()
      if (req.is('json') || req.query.json !== undefined) return res.json({ ok: true, ...r })
      res.redirect(303, `/kafka?ok=${encodeURIComponent(`round trip ok in ${r.ms}ms: published to ${r.published.partition}/${r.published.offset}, consumed from ${r.consumed.partition}/${r.consumed.offset}`)}`)
    } catch (err) {
      if (req.is('json') || req.query.json !== undefined) return res.status(500).json({ ok: false, error: err.message })
      res.redirect(303, `/kafka?err=${encodeURIComponent('round trip failed: ' + err.message)}`)
    }
  })

  startConsumer()
}

module.exports = { routes }
