const express = require('express')
const app = express()
const port = 3000

app.get('/', (req, res) => {
	console.log("Request got at", new Date().toISOString())
	res.send('Hello there from backend dir. ENV: ' + process.env.ENV)
})

app.listen(port, () => {
	console.log("\nWeb service args:", process.argv)
	console.log("\nWeb service env:", process.env.ENV)
	console.log("\nWeb service DB host:", process.env.DB_HOST)
	console.log("\nWeb service started on port:", port)
})

