//go:build clustertest

package cluster

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/netip"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"

	networkapi "github.com/moby/moby/api/types/network"
)

// Docker Desktop allocates automatic ports in its VM, independently of macOS
// sockets. Reserve host-checked ports below macOS's default ephemeral range.
// Keep their lease until container removal, including while stopped for restart.
// Containers and their published bindings are still owned by Testcontainers.
var hostPortLeases = struct {
	sync.Mutex
	used map[int]bool
}{used: make(map[int]bool)}

type hostPorts struct {
	bindings  networkapi.PortMap
	listeners []net.Listener
	numbers   []int
}

func reserveHostPorts(daemon string, exposed []string) (*hostPorts, error) {
	if runtime.GOOS != "darwin" || !strings.HasPrefix(daemon, "unix://") || os.Getenv("TESTCONTAINERS_HOST_OVERRIDE") != "" {
		return nil, nil
	}
	reserved := &hostPorts{bindings: make(networkapi.PortMap, len(exposed))}
	for _, port := range exposed {
		number, listener, err := reserveHostPort()
		if err != nil {
			reserved.release()
			return nil, err
		}
		reserved.numbers = append(reserved.numbers, number)
		reserved.listeners = append(reserved.listeners, listener)
		reserved.bindings[networkapi.MustParsePort(port)] = []networkapi.PortBinding{{
			HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: strconv.Itoa(number),
		}}
	}
	return reserved, nil
}

func reserveHostPort() (int, net.Listener, error) {
	hostPortLeases.Lock()
	defer hostPortLeases.Unlock()
	const first, count = 20000, 10000
	start := rand.IntN(count)
	var last error
	for offset := range count {
		number := first + (start+offset)%count
		if hostPortLeases.used[number] {
			continue
		}
		listener, err := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", number))
		if err != nil {
			last = err
			continue
		}
		hostPortLeases.used[number] = true
		return number, listener, nil
	}
	return 0, nil, fmt.Errorf("no free Docker Desktop host port: %w", errors.Join(errors.New("port range exhausted"), last))
}

// Docker must bind the ports at Start. Release the sockets immediately before
// that handoff; retain the in-process lease across all retained restarts.
func (p *hostPorts) handoff() {
	if p == nil {
		return
	}
	for _, listener := range p.listeners {
		_ = listener.Close()
	}
	p.listeners = nil
}

func (p *hostPorts) release() {
	if p == nil {
		return
	}
	p.handoff()
	hostPortLeases.Lock()
	defer hostPortLeases.Unlock()
	for _, number := range p.numbers {
		delete(hostPortLeases.used, number)
	}
	p.numbers = nil
}
