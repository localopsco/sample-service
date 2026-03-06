const express = require('express')
const { formatRelative } = require("date-fns");

const app = express()
const port = 3000

const serviceStartTime = new Date()

app.get('/', (req, res) => {
	console.log("Request received at", new Date().toISOString())
	console.log("Processing at", new Date().toISOString())
	res.json({
		message: "hello. this is a demo web service. lalala",
		env: process.env.ENV,
		startAt: formatRelative(serviceStartTime, new Date()),
		secret: process.env.SECRET_KEY,
	})
})

app.listen(port, () => {
	console.log("\nWeb service arg:", process.argv)
	console.log("\nWeb service env:", process.env.ENV)
	console.log("\nWeb service DB host:", process.env.DB_HOST)
	console.log("\nWeb service started on port:", port)
})

