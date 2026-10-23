package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
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
		[]postgres.Param{{OID: pgtype.OIDInt8Array, Value: pgtype.AppendInt8Array(nil, keys)}},
		postgres.QueryConfig{BatchRows: 1024},
	)

	for b := range src.Stream() {
		for _, row := range b.Items {
			idBytes, _ := row.Value(0)
			id, err := pgtype.DecodeInt8(idBytes)
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
		buf = postgres.AppendField(buf, pgtype.AppendInt8(nil, int64(i)))
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

// A server error is a struct with its SQLSTATE, which is the only part stable
// across versions, locales and rewordings. A caller given only a message ends
// up matching on text that changes with the server's language.
func ExampleError() {
	// What Exec returns for a unique-constraint violation, once the caller
	// has wrapped it on the way up.
	err := fmt.Errorf("insert order 42: %w", &postgres.Error{
		Severity: "ERROR", Code: "23505", Message: "duplicate key value", Constraint: "orders_pkey",
	})
	var e *postgres.Error
	if !errors.As(err, &e) {
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

// ExampleConnect is the shape of a connection, from an address to a Conn
// that answers queries.
//
// The recipe — DialTLS here, DialTCP or one of your own elsewhere — says how
// to reach the server; the context bounds the dial and the handshake, and
// nothing of it stays on the connection. Everything from the startup packet
// onwards is this package's, including the SCRAM-SHA-256 exchange the server
// will almost certainly ask for.
func ExampleConnect() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := postgres.Connect(ctx, postgres.DialTLS("db.internal:5432", nil), postgres.Config{
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := postgres.Connect(ctx, postgres.DialTCP("localhost:5432"), postgres.Config{
		User:     "postgres",
		Password: os.Getenv("PGPASSWORD"),
	})
	if err != nil {
		panic(err)
	}
	return conn
}
