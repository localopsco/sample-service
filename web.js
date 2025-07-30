const express = require('express')
const { formatRelative } = require("date-fns");

const app = express()
const port = 3000

const serviceStartTime = new Date()

app.get('/', (req, res) => {
	console.log("Request received at", new Date().toISOString())
	console.log("Processing request at", new Date().toISOString())
	res.send(`<h2>Hello..</h2> Started at: ${formatRelative(serviceStartTime, new Date())}. <br/> Env:${JSON.stringify(process.env, null, 2)}`)
})

app.listen(port, () => {
	console.log("\nWeb service arg:", process.argv)
	console.log("\nWeb service env:", process.env.ENV)
	console.log("\nWeb service DB host:", process.env.DB_HOST)
	console.log("\nWeb service started on port:", port)
})

