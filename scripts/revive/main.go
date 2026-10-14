// Command revive lints explicit source files while preserving package
// boundaries.
package main

import (
	"cmp"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/mgechev/revive/config"
	"github.com/mgechev/revive/lint"
)

const analyzerConcurrency = 128

type commandOptions struct {
	configPath, formatter string
	files                 []string
}

type analyzerSettings struct {
	config *lint.Config
	rules  []lint.Rule
}

func main() {
	configPath := flag.String("config", "revive.toml", "revive configuration")
	formatter := flag.String("formatter", "friendly", "friendly or json output")
	flag.Parse()
	if err := run(commandOptions{configPath: *configPath, formatter: *formatter, files: flag.Args()}); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(options commandOptions) error {
	if options.formatter != "friendly" && options.formatter != "json" {
		return fmt.Errorf("unknown formatter %q", options.formatter)
	}
	findings, err := collectFindings(options.configPath, options.files)
	if err != nil {
		return err
	}
	if err := outputFindings(findings, options.formatter); err != nil {
		return err
	}
	if len(findings) != 0 {
		return fmt.Errorf("revive: %d findings", len(findings))
	}
	return nil
}

func collectFindings(configPath string, files []string) ([]lint.Failure, error) {
	settings, err := loadSettings(configPath)
	if err != nil {
		return nil, err
	}
	analyzer := lint.New(os.ReadFile, analyzerConcurrency)
	failures, err := analyzer.Lint(packageFiles(files), settings.rules, *settings.config)
	if err != nil {
		return nil, err
	}
	findings := filterFindings(failures, settings.config.Confidence)
	slices.SortFunc(findings, func(a, b lint.Failure) int {
		left, right := a.Position.Start, b.Position.Start
		return cmp.Or(cmp.Compare(left.Filename, right.Filename), cmp.Compare(left.Offset, right.Offset))
	})
	return findings, nil
}

func outputFindings(findings []lint.Failure, formatter string) error {
	if formatter == "json" {
		if err := json.NewEncoder(os.Stdout).Encode(findings); err != nil {
			return err
		}
	} else {
		for _, failure := range findings {
			p := failure.Position.Start
			fmt.Printf("%s:%d:%d: %s (%s)\n", p.Filename, p.Line, p.Column, failure.Failure, failure.RuleName)
		}
	}
	return nil
}

func loadSettings(configPath string) (analyzerSettings, error) {
	conf, err := config.GetConfig(configPath)
	if err != nil {
		return analyzerSettings{}, err
	}
	rules, err := config.GetLintingRules(conf, nil)
	if err != nil {
		return analyzerSettings{}, err
	}
	return analyzerSettings{config: conf, rules: rules}, nil
}

func packageFiles(files []string) [][]string {
	grouped := groupFiles(files)
	directories := make([]string, 0, len(grouped))
	for dir := range grouped {
		directories = append(directories, dir)
	}
	slices.Sort(directories)
	packages := make([][]string, 0, len(directories))
	for _, dir := range directories {
		packages = append(packages, grouped[dir])
	}
	return packages
}

func filterFindings(failures <-chan lint.Failure, confidence float64) []lint.Failure {
	findings := []lint.Failure{}
	for failure := range failures {
		if failure.Confidence >= confidence {
			findings = append(findings, failure)
		}
	}
	return findings
}

func groupFiles(files []string) map[string][]string {
	grouped := map[string][]string{}
	for _, name := range files {
		grouped[filepath.Dir(name)] = append(grouped[filepath.Dir(name)], name)
	}
	return grouped
}
