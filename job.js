console.log("Job started", new Date().toISOString())

setTimeout(() => {
	console.log("Job ended", new Date().toISOString())
}, 30000)

