const express = require('express')
const app = express()
const port = 3000

// Logs one line per request with both UTC and IST, so a single log line answers
// "did the job fire, and which clock is the sidecar using?"
function hit(req, status) {
	const now = new Date()
	const utc = now.toISOString()
	const ist = now.toLocaleString('en-GB', { timeZone: 'Asia/Kolkata', hour12: false })
	console.log(`[CRON-HIT] path=${req.path} status=${status} utc=${utc} ist=${ist} ua=${req.get('user-agent') || '-'}`)
}

app.get('/', (req, res) => {
	hit(req, 200)
	res.send('ping-pong/cron: opscron target service (LOC-2204)\n')
})

// Fired every 2 minutes. The "does cron work at all" check.
app.get('/cron/tick', (req, res) => {
	hit(req, 200)
	res.json({ ok: true, job: 'tick', at: new Date().toISOString() })
})

// Fired once a day at a fixed wall-clock time. The timezone check.
app.get('/cron/daily', (req, res) => {
	hit(req, 200)
	res.json({ ok: true, job: 'daily', at: new Date().toISOString() })
})

// Always 500. The sidecar must log the failure and still fire the next tick.
app.get('/cron/fail', (req, res) => {
	hit(req, 500)
	res.status(500).json({ ok: false, job: 'fail', at: new Date().toISOString() })
})

app.get('/healthz', (req, res) => res.json({ ok: true }))

app.listen(port, () => {
	console.log('ping-pong/cron listening on port', port)
	console.log('container TZ:', process.env.TZ || '(unset, defaults to UTC)')
	console.log('node local time:', new Date().toString())
})
