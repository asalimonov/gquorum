package main

import (
	"fmt"
	"github.com/asalimonov/gquorum/topology"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/asalimonov/gquorum/fault"
)

// Linux process observations belong to the Docker test fixture. They are not
// part of the library status document or demo protocol.
type processResources struct {
	Identity                          topology.Identity `json:"identity"`
	At                                time.Time         `json:"at"`
	HeapBytes, ResidentBytes, CPUNS   uint64
	Goroutines, OpenFiles, LoadGroups int
}

func (a *application) registerResourceRoutes(mux *http.ServeMux, protect func(http.HandlerFunc) http.HandlerFunc) {
	mux.HandleFunc("GET /internal/test/resources", protect(func(w http.ResponseWriter, _ *http.Request) {
		if runtime.GOOS != "linux" {
			httpError(w, fault.ErrUnsupported)
			return
		}
		descriptors, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			httpError(w, err)
			return
		}
		raw, err := os.ReadFile("/proc/self/statm")
		if err != nil {
			httpError(w, err)
			return
		}
		fields := strings.Fields(string(raw))
		if len(fields) < 2 {
			httpError(w, fmt.Errorf("missing resident pages"))
			return
		}
		pages, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			httpError(w, err)
			return
		}
		var usage syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
			httpError(w, err)
			return
		}
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		cpu := uint64(usage.Utime.Sec+usage.Stime.Sec)*uint64(time.Second) + uint64(usage.Utime.Usec+usage.Stime.Usec)*uint64(time.Microsecond)
		jsonResponse(w, http.StatusOK, processResources{Identity: a.node.Identity(), At: time.Now().UTC(), HeapBytes: memory.HeapAlloc, ResidentBytes: pages * uint64(os.Getpagesize()), CPUNS: cpu, Goroutines: runtime.NumGoroutine(), OpenFiles: len(descriptors), LoadGroups: len(a.node.LoadGroupIDs())})
	}))
}
