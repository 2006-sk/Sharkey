//go:build unix

package transport

import "syscall"

// RaiseFileLimit raises the soft open-file limit to the hard limit. macOS
// defaults the soft limit to 256, which a coordinator serving hundreds of
// benchmark clients (plus pooled node connections) exceeds quickly.
func RaiseFileLimit() (uint64, error) {
	var rl syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl); err != nil {
		return 0, err
	}
	want := rl.Max
	// macOS rejects values above kern.maxfilesperproc even if Max is "unlimited".
	if want > 65536 {
		want = 65536
	}
	if rl.Cur >= want {
		return rl.Cur, nil
	}
	rl.Cur = want
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rl); err != nil {
		return 0, err
	}
	return rl.Cur, nil
}
