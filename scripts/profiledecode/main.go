// Command profiledecode converts a pprof CPU profile into lossless sampled stacks.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/google/pprof/profile"
)

type frame struct {
	Name string `json:"name"`
	File string `json:"file"`
	Line int64  `json:"line"`
}
type sample struct {
	CPU    int64   `json:"cpu_ns"`
	Count  int64   `json:"samples"`
	Frames []frame `json:"frames"` // Innermost frame first, including inline frames.
}
type output struct {
	Duration int64    `json:"duration_ns"`
	Time     int64    `json:"time_ns"`
	Period   int64    `json:"period"`
	Samples  []sample `json:"stacks"`
}

func decode(p *profile.Profile) (output, error) {
	out := output{Duration: p.DurationNanos, Time: p.TimeNanos, Period: p.Period,
		Samples: make([]sample, 0, len(p.Sample))}
	cpu, count := -1, -1
	for i, t := range p.SampleType {
		if t.Type == "cpu" && t.Unit == "nanoseconds" {
			cpu = i
		}
		if t.Type == "samples" && t.Unit == "count" {
			count = i
		}
	}
	if cpu < 0 || count < 0 {
		return out, errors.New("need a CPU profile with cpu/nanoseconds and samples/count values")
	}
	for _, s := range p.Sample {
		if s.Value[cpu] < 0 || s.Value[count] < 0 {
			return out, errors.New("negative/differential profiles are not supported")
		}
		item := sample{CPU: s.Value[cpu], Count: s.Value[count], Frames: sampleFrames(s)}
		out.Samples = append(out.Samples, item)
	}
	return out, nil
}
func sampleFrames(s *profile.Sample) []frame {
	frames := []frame{}
	for _, loc := range s.Location {
		unknown := fmt.Sprintf("[unknown@0x%x]", loc.Address)
		if len(loc.Line) == 0 {
			frames = append(frames, frame{Name: unknown})
		}
		for _, line := range loc.Line {
			f := frame{Line: line.Line, Name: unknown}
			if line.Function != nil {
				f.Name, f.File = line.Function.Name, line.Function.Filename
			}
			frames = append(frames, f)
		}
	}
	if len(frames) == 0 {
		frames = append(frames, frame{Name: "[unknown stack]"})
	}
	return frames
}

func run() error {
	if len(os.Args) != 2 {
		return errors.New("usage: profiledecode cpu.pprof")
	}
	f, err := os.Open(os.Args[1]) // #nosec G703 -- The CLI intentionally reads the caller-selected profile path.
	if err != nil {
		return err
	}
	defer f.Close()
	p, err := profile.Parse(f)
	if err != nil {
		return err
	}
	out, err := decode(p)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(out)
}
func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
