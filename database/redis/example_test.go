package redis_test

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/mlagarrigue/sluice/database/redis"
)

// Connect to a server on the local machine, with no password.
func ExampleConnect() {
	ctx := context.Background()
	conn, err := redis.Connect(ctx, redis.DialTCP("127.0.0.1:6379"), redis.Config{})
	if err != nil {
		fmt.Println(err)
		return
	}
	_ = conn.Close() // the connection is the caller's to close
}

// Look up a batch of keys in one round trip.
func ExampleConn_Pipeline() {
	ctx := context.Background()
	conn, err := redis.Connect(ctx, redis.DialTCP("127.0.0.1:6379"), redis.Config{})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()
	replies, err := conn.Pipeline(ctx, [][]string{{"GET", "user:1"}, {"GET", "user:2"}})
	if err != nil {
		fmt.Println(err) // the connection is broken: open another
		return
	}
	for _, r := range replies {
		switch {
		case r.Err() != nil:
			fmt.Println("refused:", r.Err())
		case r.Kind == redis.KindNil:
			fmt.Println("no such key")
		default:
			fmt.Println(string(r.Str))
		}
	}
}

// Send one command.
func ExampleConn_Do() {
	ctx := context.Background()
	conn, err := redis.Connect(ctx, redis.DialTCP("127.0.0.1:6379"), redis.Config{})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()
	r, err := conn.Do(ctx, "INCR", "visits")
	if err == nil && r.Err() == nil {
		fmt.Println(r.Int)
	}
}

// DialTLS and DialTCP are recipes: Connect runs them.
func ExampleDialTCP() {
	_ = redis.DialTCP("127.0.0.1:6379")
}

// Connect over TLS, verifying the server against the system's roots.
func ExampleDialTLS() {
	ctx := context.Background()
	dial := redis.DialTLS("cache.example.net:6380", &tls.Config{ServerName: "cache.example.net", MinVersion: tls.VersionTLS12})
	conn, err := redis.Connect(ctx, dial, redis.Config{User: "app", Password: "secret"})
	if err != nil {
		fmt.Println(err)
		return
	}
	_ = conn.Close() // the connection is the caller's to close
}

// Read a stream as one consumer of a group, acknowledging each batch once
// it is processed.
func ExampleConn_ReadGroup() {
	ctx := context.Background()
	conn, err := redis.Connect(ctx, redis.DialTCP("127.0.0.1:6379"), redis.Config{})
	if err != nil {
		fmt.Println(err)
		return
	}
	g, err := conn.ReadGroup(redis.GroupConfig{Stream: "orders", Group: "billing", Consumer: "worker-1"})
	if err != nil {
		fmt.Println(err)
		return
	}
	src := g.Entries(ctx)
	for b := range src.Stream() {
		ids := make([]string, len(b.Items))
		for i, e := range b.Items {
			ids[i] = e.ID // process e.Fields here
		}
		g.Ack(ids...)
	}
	if err := src.Err(); err != nil {
		fmt.Println(err)
	}
	_ = conn.Close()
}

// Ack may be called from any goroutine: here, the one that finished the
// work.
func ExampleGroup_Ack() {
	var g *redis.Group // from Conn.ReadGroup
	done := make(chan []string, 1)
	done <- []string{"1700000000000-0"}
	if g != nil {
		g.Ack(<-done...)
	}
}

// Entries reads until its context ends.
func ExampleGroup_Entries() {
	var g *redis.Group // from Conn.ReadGroup
	if g == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = g.Entries(ctx)
}
