// The build constraint below restricts this file to Unix-like systems (Linux,
// macOS, the BSDs). getrlimit/setrlimit are POSIX calls that do not exist on
// Windows, so on other platforms this file is simply not compiled.
//go:build unix

package transport

import "syscall"

// File-descriptor limits: why a network server has to care
// ----------------------------------------------------------
//
// On Unix "everything is a file": every open TCP socket, listening socket,
// regular file and pipe occupies one slot in the process's file-descriptor
// (fd) table. The kernel caps how many fds a process may hold open with the
// RLIMIT_NOFILE resource limit, which has two values:
//
//   - the SOFT limit (rl.Cur) is what is actually enforced right now;
//   - the HARD limit (rl.Max) is the ceiling an unprivileged process may raise
//     its own soft limit to (only root can raise the hard limit).
//
// When the soft limit is reached, the next accept(2), socket(2) or open(2)
// fails with EMFILE ("too many open files"). For this system that would mean:
//
//   - the coordinator cannot accept new client connections (Server.Serve sees
//     EMFILE from Accept and has to back off and retry, see isTemporaryAccept);
//   - the coordinator cannot dial storage nodes (Pool dials would fail), so
//     healthy nodes would look "down" to failure detection;
//   - the benchmark client cannot open all of its simulated connections.
//
// A coordinator needs roughly one fd per client connection plus one per
// pooled node connection plus the listener, so a few hundred benchmark
// clients blow straight through a default soft limit of 256.

// RaiseFileLimit raises the soft open-file limit to the hard limit. macOS
// defaults the soft limit to 256, which a coordinator serving hundreds of
// benchmark clients (plus pooled node connections) exceeds quickly.
//
// This is the programmatic equivalent of running `ulimit -n <hard>` in the
// shell before starting the process. Doing it in code means `make run-cluster`
// and the benchmark just work without the user having to remember that step.
//
// It returns the soft limit that is in effect afterwards. It never lowers the
// limit: if the current soft limit is already high enough it is left alone.
// Raising a soft limit up to the hard limit needs no special privileges.
func RaiseFileLimit() (uint64, error) {
	// Rlimit is the C struct { Cur, Max } filled in by the kernel.
	var rl syscall.Rlimit
	// RLIMIT_NOFILE = "max number of open file descriptors". Getrlimit is a
	// cheap syscall; this runs once at startup, so its cost is irrelevant.
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl); err != nil {
		return 0, err
	}
	// Aim for the hard limit: the most we are allowed to ask for.
	want := rl.Max
	// On macOS the hard limit is frequently reported as RLIM_INFINITY (a huge
	// number), but the kernel still enforces kern.maxfilesperproc and rejects a
	// larger soft limit with EINVAL. Clamping to 65536 keeps the request inside
	// what the kernel will accept while still being far more than we need.
	// macOS rejects values above kern.maxfilesperproc even if Max is "unlimited".
	if want > 65536 {
		want = 65536
	}
	// Already at (or above) the target: nothing to do. This also avoids
	// accidentally LOWERING a limit someone configured higher on purpose.
	if rl.Cur >= want {
		return rl.Cur, nil
	}
	// Only the soft value changes; rl.Max is passed back unchanged, so we keep
	// the ability to raise it again later.
	rl.Cur = want
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rl); err != nil {
		return 0, err
	}
	// The new limit applies to fds opened from now on; it is inherited by any
	// child processes this process starts.
	return rl.Cur, nil
}
