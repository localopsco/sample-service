console.log("args:", process.argv)
console.log("Job started", new Date().toISOString())

setTimeout(() => {
	console.log("Job ended", new Date().toISOString())
}, 25000)

