//go:build !unix

package print

func stdoutTerminalWidth() (int, bool) {
	return 0, false
}
