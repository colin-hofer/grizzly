//go:build unix

package print

import (
	"os"
	"syscall"
	"unsafe"
)

type winsize struct {
	row    uint16
	col    uint16
	xpixel uint16
	ypixel uint16
}

func stdoutTerminalWidth() (int, bool) {
	fi, err := os.Stdout.Stat()
	if err != nil {
		return 0, false
	}
	if fi.Mode()&os.ModeCharDevice == 0 {
		return 0, false
	}

	ws := winsize{}
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, os.Stdout.Fd(), uintptr(syscall.TIOCGWINSZ), uintptr(unsafe.Pointer(&ws)))
	if errno != 0 {
		return 0, false
	}
	if ws.col == 0 {
		return 0, false
	}
	return int(ws.col), true
}
