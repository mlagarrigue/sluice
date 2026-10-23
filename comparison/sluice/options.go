package main

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"time"
)

// options is the whole matrix of the internal study (docs: the comparison
// study, §8), one flag per axis, with the reference configuration R as the
// default of every flag. A campaign starts from this one binary whatever it
// measures, so two variants differ by the flag they name and by nothing else.
//
// A variant the code does not implement yet is refused by validate, out loud,
// before anything listens: a flag that silently served R would publish R's
// figure under another name.
type options struct {
	listen string
	tls    bool
	cert   string
	key    string

	// cpuprofile is where the server's CPU profile goes, written when it
	// stops; empty writes none. A diagnostic, not an axis of the study.
	cpuprofile string

	// Axis A — protocol and transport.
	transport string

	// Axis B — driver.
	driver string

	// Axis C — grouping.
	gatewaySize   int
	gatewayWithin time.Duration

	// Axis D — the transport's own batch and connection budget (httpstream).
	maxRequestsPerBatch int
	maxRequestsPerConn  int

	// Axis E — response.
	response string

	// Axis F — compression.
	compression string

	// Off the axes: how the list is encoded (§2.4, the optimised version).
	json string

	// The database behind every dynamic scenario.
	db      dbConfig
	dbConns int

	// listMax bounds what one list request may ask for.
	listMax int
}

type dbConfig struct {
	addr, user, database, password string
}

const (
	transportNetHTTPH1    = "nethttp-h1"
	transportNetHTTPH2    = "nethttp-h2"
	transportHTTPStreamH1 = "httpstream-h1"
	transportHTTPStreamH2 = "httpstream-h2"
	transportHTTPStreamH3 = "httpstream-h3"

	driverSluice = "sluice"
	driverPgx    = "pgx"

	responseBuffered = "buffered"
	responseStream   = "stream"

	jsonStd    = "std"
	jsonAppend = "append"

	// maxDBConns bounds -db-conns: PostgreSQL's own default ceiling is 100,
	// and pgxpool counts in int32.
	maxDBConns = 4096
)

func parseOptions(args []string) (options, error) {
	var o options
	fs := flag.NewFlagSet("sluice-study", flag.ContinueOnError)
	fs.StringVar(&o.listen, "listen", "127.0.0.1:8443", "address to listen on")
	fs.BoolVar(&o.tls, "tls", true, "serve TLS (R); false is cleartext HTTP/1.1, for local tooling only")
	fs.StringVar(&o.cert, "cert", "", "TLS certificate file (PEM); empty generates a self-signed ECDSA P-256 certificate at startup")
	fs.StringVar(&o.key, "key", "", "TLS key file (PEM), with -cert")
	fs.StringVar(&o.cpuprofile, "cpuprofile", "", "write the server's CPU profile to this file when it stops (SIGINT)")

	fs.StringVar(&o.transport, "transport", transportNetHTTPH1,
		"axis A: nethttp-h1 (R), nethttp-h2, httpstream-h1, httpstream-h2, httpstream-h3 (QUIC, UDP)")
	fs.StringVar(&o.driver, "driver", driverSluice,
		"axis B: sluice (R, database/postgres), pgx (jackc/pgx v5; same gateway, same connections)")
	fs.IntVar(&o.gatewaySize, "gateway-size", 64, "axis C: gateway Size, the largest batch of unit reads")
	fs.DurationVar(&o.gatewayWithin, "gateway-within", 2*time.Millisecond, "axis C: gateway Within, how long the first key of a batch waits")
	fs.IntVar(&o.maxRequestsPerBatch, "max-requests-per-batch", 0,
		"axis D: httpstream Config.MaxRequestsPerBatch; 0 is the package default (64); httpstream transports only")
	fs.IntVar(&o.maxRequestsPerConn, "max-requests-per-conn", 0,
		"axis D: httpstream Config.MaxRequestsPerConn; 0 is the package default (1024), -1 is unbounded; httpstream transports only")
	fs.StringVar(&o.response, "response", responseBuffered,
		"axis E: buffered (R), stream (the list is written and flushed batch by batch, chunked; with -compression, the encoder is flushed per batch too)")
	fs.StringVar(&o.compression, "compression", "none",
		"axis F: none (R), gzip1, gzip6, deflate6; the body is encoded when the client accepts it")

	fs.StringVar(&o.json, "json", jsonStd,
		"list encoder: std (R, encoding/json), append (a hand encoder of the list document, strconv.AppendInt; the other scenarios stay under encoding/json)")

	fs.StringVar(&o.db.addr, "db", os.Getenv("SLUICE_PG"), "PostgreSQL host:port (default $SLUICE_PG)")
	fs.StringVar(&o.db.user, "db-user", os.Getenv("SLUICE_PG_USER"), "PostgreSQL role (default $SLUICE_PG_USER)")
	fs.StringVar(&o.db.database, "db-name", os.Getenv("SLUICE_PG_DB"), "PostgreSQL database (default $SLUICE_PG_DB)")
	fs.StringVar(&o.db.password, "db-password", "", "PostgreSQL password (default $SLUICE_PG_PASSWORD, not echoed here)")
	fs.IntVar(&o.dbConns, "db-conns", 8, "connections for list and write requests; unit reads share the gateway's own connection")
	fs.IntVar(&o.listMax, "list-max", 100000, "largest limit a list request may ask for")

	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if o.db.password == "" {
		o.db.password = os.Getenv("SLUICE_PG_PASSWORD")
	}
	return o, o.validate()
}

var errNotImplemented = errors.New("not implemented")

// validate refuses what the binary cannot serve as asked. Every refusal names
// the axis whose brief will lift it: nothing here falls back to R.
func (o options) validate() error {
	switch o.transport {
	case transportNetHTTPH1, transportHTTPStreamH1:
	case transportNetHTTPH2, transportHTTPStreamH2:
		if !o.tls {
			return fmt.Errorf("-transport %s: %w without -tls (h2c is not a configuration of the study)", o.transport, errNotImplemented)
		}
	case transportHTTPStreamH3:
		if !o.tls {
			return fmt.Errorf("-transport %s: QUIC is always TLS, -tls=false has no meaning", o.transport)
		}
	default:
		return fmt.Errorf("-transport %q: unknown", o.transport)
	}
	switch o.driver {
	case driverSluice, driverPgx:
	default:
		return fmt.Errorf("-driver %q: unknown", o.driver)
	}
	if o.gatewaySize <= 0 || o.gatewayWithin <= 0 {
		return errors.New("-gateway-size and -gateway-within must be positive")
	}
	if !o.httpstream() && (o.maxRequestsPerBatch != 0 || o.maxRequestsPerConn != 0) {
		return fmt.Errorf("-max-requests-per-batch and -max-requests-per-conn only apply to httpstream transports, not %s", o.transport)
	}
	if o.maxRequestsPerBatch < 0 || o.maxRequestsPerConn < -1 {
		return errors.New("-max-requests-per-batch must be >= 0 and -max-requests-per-conn >= -1")
	}
	switch o.response {
	case responseBuffered, responseStream:
	default:
		return fmt.Errorf("-response %q: unknown", o.response)
	}
	switch o.compression {
	case codecNone, codecGzip1, codecGzip6, codecDeflate6:
	default:
		return fmt.Errorf("-compression %q: unknown", o.compression)
	}
	switch o.json {
	case jsonStd, jsonAppend:
	default:
		return fmt.Errorf("-json %q: unknown", o.json)
	}
	if o.db.addr == "" || o.db.user == "" {
		return errors.New("-db and -db-user are required (or SLUICE_PG and SLUICE_PG_USER)")
	}
	if o.dbConns <= 0 || o.listMax <= 0 {
		return errors.New("-db-conns and -list-max must be positive")
	}
	if o.dbConns > maxDBConns {
		return fmt.Errorf("-db-conns must be at most %d", maxDBConns)
	}
	if (o.cert == "") != (o.key == "") {
		return errors.New("-cert and -key go together")
	}
	return nil
}

func (o options) httpstream() bool {
	switch o.transport {
	case transportHTTPStreamH1, transportHTTPStreamH2, transportHTTPStreamH3:
		return true
	}
	return false
}

// perConn translates the flag's -1 into the only unbounded value httpstream
// has (docs/benchmarks.md, "The connection budget and the batch bound").
func (o options) perConn() int {
	if o.maxRequestsPerConn == -1 {
		return math.MaxInt
	}
	return o.maxRequestsPerConn
}
