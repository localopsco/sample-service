const express = require('express')
const app = express()
const port = 3001
const promClient = require('prom-client');

app.get('/', (req, res) => {
	res.send('Internal service here; Hello there')
})

app.listen(port, () => {
	console.log("Internal service args:", process.argv)
	console.log("Internal service running on port:", port)
})



// Metrics server — separate port
const metrics = express();

promClient.collectDefaultMetrics();

metrics.get('/metrics', async (req, res) => {
  console.log("Metrics request received at", new Date().toISOString())
  res.set('Content-Type', promClient.register.contentType);
  res.end(await promClient.register.metrics());
});

metrics.listen(9090, () => console.log('Metrics on port 9090'));