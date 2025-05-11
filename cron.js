console.log("args:", process.argv)
console.log("Cron job started", new Date().toISOString())

setTimeout(() => {
	console.log("Cron job end", new Date().toISOString())
}, 10*1000)

