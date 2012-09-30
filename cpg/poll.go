package cpg

// #include <poll.h>
import "C"

const (
	PollIn  = 1 << iota
	PollOut = 1 << iota
)

func pollC2G(fl C.short) uint {
	var ret uint

	if fl&C.POLLIN != 0 {
		ret |= PollIn
	}
	if fl&C.POLLOUT != 0 {
		ret |= PollOut
	}

	return ret
}

func pollG2C(fl uint) C.short {
	var ret C.short

	if fl&PollIn != 0 {
		ret |= C.POLLIN
	}
	if fl&PollOut != 0 {
		ret |= C.POLLOUT
	}

	return ret
}

func poll(fds map[int]uint, ctl ctlCh) <-chan int {
	n := len(fds)

	ch := make(chan int)

	go func() {
		pollfd := make([]C.struct_pollfd, n)

		for {
			idx := 0
			for fd, fl := range fds {
				if fl == 0 {
					continue
				}

				pollfd[idx].fd = C.int(fd)
				pollfd[idx].events = pollG2C(fl)
				pollfd[idx].revents = 0
				idx++
			}

			ret := C.poll(&pollfd[0], C.nfds_t(n), -1)
			if ret < 0 {
				return
			}

			select {
			case _, ok := <-ctl:
				if  !ok {
					return
				}

			default:
			}

			for i, _ := range pollfd {
				if (pollfd[i].revents & pollfd[i].events) != 0 {
					ch <- int(pollfd[i].fd)
				}
			}
		}
	}()

	return ch
}
