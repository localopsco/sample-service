const express = require('express')
const app = express()
const port = 3000

app.get('/', (req, res) => {
	console.log("Request got at", new Date().toISOString())
	res.send(`
		<div style="font-family: sans-serif; max-width: 600px; margin: 40px auto; padding: 30px; background: #f0f4ff; border: 2px solid #4a6cf7; border-radius: 12px; box-shadow: 0 4px 12px rgba(74,108,247,0.15);">
			<h1 style="color: #4a6cf7; margin-top: 0;">Ping Pong Web 🏓</h1>
			<p style="color: #555; font-size: 1.1em;">Served from <code>ping-pong/web/</code></p>
			<p style="color: #888;">ENV: ${process.env.ENV}</p>
		</div>
	`)
})

app.listen(port, () => {
	console.log("\nWeb service args:", process.argv)
	console.log("\nWeb service env:", process.env.ENV)
	console.log("\nWeb service started on port:", port)
})
