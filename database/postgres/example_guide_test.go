package postgres_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"time"

	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
)

// One row of a binary COPY: the field count, then each field with its
// length — here an int8 and a NULL, whose length is -1 and which has no
// bytes.
func ExampleAppendTupleHeader() {
	var scratch []byte
	buf := postgres.AppendTupleHeader(nil, 2)
	scratch = pgtype.AppendInt8(scratch[:0], 7)
	buf = postgres.AppendField(buf, scratch)
	buf = postgres.AppendFieldNull(buf)
	fmt.Printf("% x\n", buf)
	// Output: 00 02 00 00 00 08 00 00 00 00 00 00 00 07 ff ff ff ff
}

// A field is its length, four bytes, then its bytes: the value is written by
// its type's encoder, the framing by this function.
func ExampleAppendField() {
	fmt.Printf("% x\n", postgres.AppendField(nil, pgtype.AppendText(nil, "ok")))
	// Output: 00 00 00 02 6f 6b
}

// A connection in clear, to a server on the same host or a private network.
// Compiled, not run: it needs a server.
func ExampleDialTCP() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := postgres.Connect(ctx, postgres.DialTCP("localhost:5432"), postgres.Config{
		User:     "orders",
		Password: os.Getenv("PGPASSWORD"),
	})
	if err != nil {
		fmt.Println("connecting:", err)
		return
	}
	defer func() { _ = conn.Close() }()
	fmt.Println("connected to", conn.Parameter("server_version"))
}

// TLS negotiated as every PostgreSQL server does it: an SSLRequest, then the
// handshake, refused rather than downgraded when the server declines. A nil
// config checks the certificate against the system roots and the host of
// the address. Compiled, not run.
func ExampleDialTLS() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := postgres.Connect(ctx, postgres.DialTLS("db.internal:5432", nil), postgres.Config{
		User:     "orders",
		Password: os.Getenv("PGPASSWORD"),
	})
	if err != nil {
		fmt.Println("connecting:", err)
		return
	}
	defer func() { _ = conn.Close() }()
	fmt.Println("connected to", conn.Parameter("server_version"))
}

// TLS from the first byte, without the SSLRequest round trip: PostgreSQL 17
// and later. Compiled, not run.
func ExampleDialTLSDirect() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dial := postgres.DialTLSDirect("db.internal:5432", &tls.Config{MinVersion: tls.VersionTLS13})
	conn, err := postgres.Connect(ctx, dial, postgres.Config{
		User:     "orders",
		Password: os.Getenv("PGPASSWORD"),
	})
	if err != nil {
		fmt.Println("connecting:", err)
		return
	}
	defer func() { _ = conn.Close() }()
	fmt.Println("connected to", conn.Parameter("server_version"))
}
