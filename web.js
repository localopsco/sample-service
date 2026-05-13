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
  const message = funMessages[Math.floor(Math.random() * funMessages.length)]
  console.log("Request received at", new Date().toISOString())
  console.log("Processing at", new Date().toISOString())
  res.send(`
		<div style="font-family: sans-serif; max-width: 600px; margin: 40px auto; padding: 30px; background: #f0f4ff; border: 2px solid #4a6cf7; border-radius: 12px; box-shadow: 0 4px 12px rgba(74,108,247,0.15);">
			<h1 style="color: #4a6cf7; margin-top: 0;">Ping Pong Service 🏓</h1>
			<p style="color: #555; font-size: 1.1em;">${message}</p>
			<p style="color: #888; font-style: italic;">Know your IP and Browser</p>
			<table style="width: 100%; border-collapse: collapse;">
				<tr><td style="padding: 10px; border-bottom: 1px solid #ccd5f7; font-weight: bold; color: #333;">Environment</td><td style="padding: 10px; border-bottom: 1px solid #ccd5f7;">${process.env.ENV}</td></tr>
				<tr><td style="padding: 10px; border-bottom: 1px solid #ccd5f7; font-weight: bold; color: #333;">Started</td><td style="padding: 10px; border-bottom: 1px solid #ccd5f7;">${formatRelative(serviceStartTime, new Date())}</td></tr>
				<tr><td style="padding: 10px; border-bottom: 1px solid #ccd5f7; font-weight: bold; color: #333;">Browser</td><td style="padding: 10px; border-bottom: 1px solid #ccd5f7;">${req.get('User-Agent')}</td></tr>
				<tr><td style="padding: 10px; font-weight: bold; color: #333;">IP</td><td style="padding: 10px;">${req.ip}</td></tr>
			</table>
		</div>
	`)
})

app.get('/crash', (req, res) => {
  const fs = require('fs')
  fs.readFileSync('/nonexistent/file/that/does/not/exist.txt')
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

