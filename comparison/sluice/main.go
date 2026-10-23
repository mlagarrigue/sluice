// Command sluice is the Sluice.go server of the internal study: the four
// scenarios of level 1 in the reference configuration R, every axis of the
// study behind a flag. See ../README.md.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime/pprof"
)

func main() {
	os.Exit(mainErr())
}

func mainErr() int {
	o, err := parseOptions(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return 2
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "sluice-study:", err)
		return 2
	}
	if o.cpuprofile != "" {
		f, err := os.Create(o.cpuprofile)
		if err != nil {
			fmt.Fprintln(os.Stderr, "sluice-study:", err)
			return 2
		}
		defer func() { _ = f.Close() }()
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintln(os.Stderr, "sluice-study:", err)
			return 2
		}
		defer pprof.StopCPUProfile()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, o); err != nil {
		fmt.Fprintln(os.Stderr, "sluice-study:", err)
		return 1
	}
	return 0
}

func run(ctx context.Context, o options) (err error) {
	a, err := newApp(o)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, a.Close()) }()
	if o.transport == transportHTTPStreamH3 {
		pc, err := net.ListenPacket("udp", o.listen)
		if err != nil {
			return err
		}
		tlsCfg, err := tlsConfig(o)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "sluice-study: %s on udp %s\n", o.transport, pc.LocalAddr())
		return serveH3(ctx, o, a, pc, tlsCfg)
	}
	ln, err := net.Listen("tcp", o.listen)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "sluice-study: %s on %s (tls=%v)\n", o.transport, ln.Addr(), o.tls)
	return serve(ctx, o, a, ln)
}
