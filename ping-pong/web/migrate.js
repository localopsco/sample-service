// One-shot init job for the ping-pong web service.
//
// Declared in ops.json under `init` with `"once": true`, which makes LocalOps
// run this as an initContainer before the web container starts — and, because of
// `once`, only on the single pod that wins leader election. That `once` flag is
// what pulls the leader-elector sidecar into the pod.
//
// The two log lines below are the point of the exercise: they report which uid
// the init container actually runs as, and whether /tmp is writable. The chart
// mounts an emptyDir at /tmp so that a non-root init container has somewhere to
// download the static jq binary it needs.

const fs = require('fs')

console.log("Init job starting at", new Date().toISOString())
console.log("Running as uid:", process.getuid(), "gid:", process.getgid())
console.log("Args:", process.argv)

try {
	fs.writeFileSync('/tmp/init-probe', 'ok')
	fs.unlinkSync('/tmp/init-probe')
	console.log("/tmp is writable ✅")
} catch (e) {
	console.log("/tmp is NOT writable ❌ —", e.message)
}

console.log("Pretending to migrate something important...")

setTimeout(() => {
	console.log("Init job done 🏓")
	process.exit(0)
}, 2000)
