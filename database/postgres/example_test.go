package postgres_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/mlagarrigue/sluice/database/postgres"
)

// The claim this package exists for: a thousand keys leave as one parameter,
// in one round trip, and no value ever touches SQL text.
//
// This example needs a server, so it is compiled but not run — which is what
// keeps it from rotting while the API changes around it.
func Example_anyArray() {
	conn := mustConnect()
	defer func() { _ = conn.Close() }()

	keys := make([]int64, 1000)
	for i := range keys {
		keys[i] = int64(i)
	}

	// One parameter carrying every key. The alternative — a thousand
	// placeholders, or worse, the values interpolated into the text — is the
	// injection surface this removes rather than guards.
	src := conn.Query(context.Background(),
		"SELECT id, total FROM orders WHERE id = ANY($1)",
		[][]byte{postgres.AppendInt8Array(nil, keys)},
		postgres.QueryConfig{BatchRows: 1024, ParamOIDs: []uint32{postgres.OIDInt8Array}},
	)

	for b := range src.Stream() {
		for _, row := range b.Items {
			idBytes, _ := row.Value(0)
			id, err := postgres.DecodeInt8(idBytes)
			if err != nil {
				fmt.Println("decoding id:", err)
				return
			}
			fmt.Println(id)
		}
	}
	// Consume, then check: a query that failed to start produced no row to
	// attach an error to, and one that failed at the end produced all of them.
	if err := src.Err(); err != nil {
		fmt.Println("query failed:", err)
	}
}

// A bulk load, cut into transactions of the caller's choosing. The size is
// required because the obvious default — one transaction around everything —
// is not a slow option but a defect at scale: nothing is durable until the
// end, restart is all or nothing, and a long transaction pins the vacuum
// horizon for the whole database.
func Example_copy() {
	conn := mustConnect()
	defer func() { _ = conn.Close() }()

	cp, err := conn.CopyFrom(context.Background(),
		"COPY orders (id, label) FROM STDIN WITH (FORMAT BINARY)",
		postgres.CopyConfig{RowsPerTx: 100_000},
	)
	if err != nil {
		fmt.Println("starting the copy:", err)
		return
	}

	// Rows are built into a buffer the caller owns and reuses, so a batch
	// costs no allocation per row.
	var buf []byte
	for i := range 1000 {
		buf = buf[:0]
		buf = postgres.AppendTupleHeader(buf, 2)
		buf = postgres.AppendField(buf, postgres.AppendInt8(nil, int64(i)))
		buf = postgres.AppendFieldNull(buf) // NULL is not the empty string
		if err := cp.WriteTuples(buf, 1); err != nil {
			fmt.Println("writing:", err)
			return
		}
	}
	if err := cp.Close(); err != nil {
		fmt.Println("finishing:", err)
	}
}

// numeric is kept exactly, because a column is declared numeric precisely when
// a float would lose what matters. Every lossy reading has to be named.
func ExampleNumeric() {
	// A value with more significant digits than a float64 can hold.
	n, err := postgres.ParseNumeric("12345678901234567890.12345678901234567890")
	if err != nil {
		fmt.Println(err)
		return
	}

	// Through the wire and back, unchanged.
	back, err := postgres.DecodeNumeric(postgres.AppendNumeric(nil, n))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("exact: ", back.String())
	fmt.Println("as a float64, which loses what the column was chosen to keep:")
	fmt.Printf("        %.6f...\n", back.Float64())

	// The display scale is data, not decoration: numeric(12,2) holding ten is
	// "10.00", and that is what an invoice prints.
	scaled, _ := postgres.ParseNumeric("10.00")
	fmt.Println("scale: ", scaled.String())
	// Output:
	// exact:  12345678901234567890.12345678901234567890
	// as a float64, which loses what the column was chosen to keep:
	//         12345678901234567168.000000...
	// scale:  10.00
}

// A server error is a struct with its SQLSTATE, which is the only part stable
// across versions, locales and rewordings. A caller given only a message ends
// up matching on text that changes with the server's language.
func ExampleError() {
	// What the server sends for a unique-constraint violation, as this package
	// parses it.
	body := []byte("SERROR\x00C23505\x00Mduplicate key value\x00norders_pkey\x00\x00")
	e, err := postgres.ParseError(body)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("code:      ", e.Code)
	fmt.Println("constraint:", e.Constraint)
	fmt.Println("matchable: ", e.Code == "23505")
	// Output:
	// code:       23505
	// constraint: orders_pkey
	// matchable:  true
}

// ExampleStartup is the shape of a connection, from a socket to a Conn that
// answers queries.
//
// The transport stays the caller's — which host, which timeout, whether it is
// wrapped in TLS — because those are deployment decisions a library cannot see.
// Everything from the startup packet onwards is this package's, including the
// SCRAM-SHA-256 exchange the server will almost certainly ask for.
func ExampleStartup() {
	nc, err := net.DialTimeout("tcp", "db.internal:5432", 5*time.Second)
	if err != nil {
		fmt.Println("dialling:", err)
		return
	}
	// The handshake takes its deadline from the connection, so this bounds it.
	if err := nc.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		fmt.Println(err)
		return
	}

	conn, err := postgres.Startup(nc, postgres.StartupConfig{
		User:     "orders",
		Database: "orders",
		Password: os.Getenv("PGPASSWORD"),
		Params:   map[string]string{"application_name": "orders-etl"},
	})
	if err != nil {
		// A wrong password arrives as a *postgres.Error with SQLSTATE 28P01;
		// a method this package declines arrives as ErrAuthUnsupported. They
		// need different fixes, so they are different errors.
		fmt.Println("connecting:", err)
		return
	}
	defer func() { _ = conn.Close() }()

	fmt.Println("connected to", conn.Parameter("server_version"))
}

// mustConnect stands in for the caller's own connection in the examples above,
// which need a server and are therefore compiled rather than run.
func mustConnect() *postgres.Conn {
	nc, err := net.DialTimeout("tcp", "localhost:5432", 5*time.Second)
	if err != nil {
		panic(err)
	}
	conn, err := postgres.Startup(nc, postgres.StartupConfig{
		User:     "postgres",
		Password: os.Getenv("PGPASSWORD"),
	})
	if err != nil {
		panic(err)
	}
	return conn
}
