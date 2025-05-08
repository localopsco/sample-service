const express = require('express')
const app = express()
const port = 3001

app.get('/', (req, res) => {
	res.send('Internal service here; Hello there')
})

app.listen(port, () => {
	console.log("Internal service args:", process.argv)
	console.log("Internal service running on port:", port)
})

