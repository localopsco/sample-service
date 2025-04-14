const express = require('express')
const app = express()
const port = 3001

app.get('/', (req, res) => {
	res.send('Internal service; Hello there')
})

app.listen(port, () => {
	console.log("Sample app started on port:", port)
})

