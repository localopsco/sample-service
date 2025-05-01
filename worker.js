console.log("args:", process.argv)
console.log("worker started...")

setInterval(() => {
	console.log("working...", new Date().toISOString())
}, 2000)

