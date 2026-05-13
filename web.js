const express = require('express')
const { formatRelative } = require("date-fns");
const promClient = require('prom-client');

const app = express()
const port = 3000

const serviceStartTime = new Date()

const funMessages = [
  "I'm not a regular server, I'm a cool server 😎",
  "Serving bytes and good vibes since startup ✨",
  "You again? I'm flattered 🥰",
  "Plot twist: the real API was the friends we made along the way 🤝",
  "I run on coffee and HTTP requests ☕",
  "Error 200: Everything is actually fine for once 🎉",
  "Beep boop, I am definitely not a robot 🤖",
  "Welcome! I've been expecting you... just kidding, I have no memory 🧠",
  "This response was handcrafted with love and JavaScript 💙",
  "I could've been a database, but I chose a life of service 🫡",
]

app.get('/', (req, res) => {
	console.log("Request received at", new Date().toISOString())
	console.log("Processing request at", new Date().toISOString())
	res.send(`<h2>Hello from Test web Env:${process.env.ENV}.</h2> Started at: ${formatRelative(serviceStartTime, new Date())}`)
})

app.listen(port, () => {
  console.log("\nWeb service arg:", process.argv)
  console.log("\nWeb service env:", process.env.ENV)
  console.log("\nWeb service DB host:", process.env.DB_HOST)
  console.log("\nWeb service started on port:", port)
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

