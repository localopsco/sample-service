console.log("args:", process.argv)
console.log("worker started...")

setInterval(() => {
	consol.log("working...", new Date().toISOString())
}, 2000)

