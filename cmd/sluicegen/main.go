package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	typeName := flag.String("type", "", "name of the struct type to generate a hydrator for")
	input := flag.String("in", "", "source file holding the type (default: $GOFILE, set by go:generate)")
	output := flag.String("out", "", "file to write (default: <type>_hydrate.go, lowercased)")
	flag.Parse()

	if err := run(*typeName, *input, *output); err != nil {
		fmt.Fprintln(os.Stderr, "sluicegen:", err)
		os.Exit(1)
	}
}

func run(typeName, input, output string) error {
	if typeName == "" {
		return errors.New("-type is required")
	}
	if input == "" {
		// go:generate sets GOFILE to the file holding the directive, which is
		// where the type is in the overwhelming majority of cases.
		input = os.Getenv("GOFILE")
	}
	if input == "" {
		return errors.New("-in is required outside go:generate")
	}
	if output == "" {
		output = filepath.Join(filepath.Dir(input), strings.ToLower(typeName)+"_hydrate.go")
	}

	// The path is the caller's, which is what a generator invoked from a
	// go:generate line is for. There is no boundary here to cross.
	src, err := os.ReadFile(input) //nolint:gosec // G304: reading the file named on the command line is the job
	if err != nil {
		return err
	}
	generated, err := Generate(src, input, typeName)
	if err != nil {
		return err
	}
	// 0o644: generated source is source, and it is read by everyone who reads
	// the rest of the package.
	if err := os.WriteFile(output, generated, 0o644); err != nil { //nolint:gosec // G306: source files are world-readable
		return err
	}
	return nil
}
