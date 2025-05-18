const express = require('express')
const { formatRelative } = require("date-fns");

const app = express()
const port = 3000

const serviceStartTime = new Date()

app.get('/', (req, res) => {
	console.log("Request received at", new Date().toISOString())
	console.log("Processing request at", new Date().toISOString())
	res.send(`Hello.. Env:${process.env.ENV} \n\nStarted at: ${formatRelative(serviceStartTime, new Date())}`)
})

app.listen(port, () => {
	console.log("\nWeb service args:", process.argv)
	console.log("\nWeb service env:", process.env.ENV)
	console.log("\nWeb service DB host:", process.env.DB_HOST)
	console.log("\nWeb service started on port:", port)
})

