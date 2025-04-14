console.log("Cron job started", new Date().toISOString())

setInterval(() => {
	console.log("Cron job ended", new Date().toISOString())
}, 2000)

