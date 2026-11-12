package nats_test

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/mlagarrigue/sluice/messaging/nats"
)

// Connect to a server on the local machine.
func ExampleConnect() {
	ctx := context.Background()
	conn, err := nats.Connect(ctx, nats.DialTCP("127.0.0.1:4222"), nats.Config{Name: "billing"})
	if err != nil {
		fmt.Println(err)
		return
	}
	_ = conn.Close()
}

// DialTCP is a recipe: Connect runs it.
func ExampleDialTCP() {
	_ = nats.DialTCP("127.0.0.1:4222")
}

// Read a durable pull consumer in batches, acknowledging each message once
// handled.
func ExampleConn_Consume() {
	ctx := context.Background()
	conn, err := nats.Connect(ctx, nats.DialTCP("127.0.0.1:4222"), nats.Config{})
	if err != nil {
		fmt.Println(err)
		return
	}
	k, err := conn.Consume(nats.ConsumerConfig{Stream: "ORDERS", Consumer: "billing"})
	if err != nil {
		fmt.Println(err)
		return
	}
	src := k.Messages(ctx)
	for b := range src.Stream() {
		for _, m := range b.Items {
			k.Ack(m.Reply) // after handling m.Data
		}
	}
	if err := src.Err(); err != nil {
		fmt.Println(err)
	}
	_ = conn.Close()
}

// Messages reads until its context ends.
func ExampleConsumer_Messages() {
	var k *nats.Consumer // from Conn.Consume
	if k == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = k.Messages(ctx)
}

// Ack may be called from any goroutine: here, the one that handled the
// message.
func ExampleConsumer_Ack() {
	var k *nats.Consumer // from Conn.Consume
	done := make(chan string, 1)
	done <- "$JS.ACK.ORDERS.billing.1.1.1.1700000000000000000.0"
	if k != nil {
		k.Ack(<-done)
	}
}

// Publish a batch in one write.
func ExampleConn_Publish() {
	ctx := context.Background()
	conn, err := nats.Connect(ctx, nats.DialTCP("127.0.0.1:4222"), nats.Config{})
	if err != nil {
		fmt.Println(err)
		return
	}
	err = conn.Publish(ctx, []nats.Message{
		{Subject: "ORDERS.created", Data: []byte(`{"id":1}`)},
		{Subject: "ORDERS.created", Data: []byte(`{"id":2}`)},
	})
	fmt.Println(err)
	_ = conn.Close()
}

// Create a stream through the JetStream API.
func ExampleConn_Request() {
	ctx := context.Background()
	conn, err := nats.Connect(ctx, nats.DialTCP("127.0.0.1:4222"), nats.Config{})
	if err != nil {
		fmt.Println(err)
		return
	}
	reply, err := conn.Request(ctx, "$JS.API.STREAM.CREATE.ORDERS", []byte(`{"name":"ORDERS","subjects":["ORDERS.>"]}`))
	if err == nil {
		fmt.Println(string(reply.Data))
	}
	_ = conn.Close()
}

// Publish a batch and wait for the stream to store each message.
func ExampleConn_PublishAcked() {
	ctx := context.Background()
	conn, err := nats.Connect(ctx, nats.DialTCP("127.0.0.1:4222"), nats.Config{})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()
	acks, err := conn.PublishAcked(ctx, []nats.Message{
		{Subject: "ORDERS.created", Data: []byte(`{"id":1}`)},
		{Subject: "ORDERS.created", Data: []byte(`{"id":2}`)},
	})
	for i, a := range acks {
		if a.Err != nil {
			fmt.Println(i, a.Err) // not stored: publish it again, or report it
		}
	}
	if acks == nil && err != nil {
		fmt.Println(err) // the connection broke: nothing is known
	}
}

// Connect over TLS: the server sends its INFO in clear, then the connection
// is upgraded.
func ExampleConfig_tls() {
	ctx := context.Background()
	conn, err := nats.Connect(ctx, nats.DialTCP("nats.example.com:4222"), nats.Config{
		TLS: &tls.Config{ServerName: "nats.example.com", MinVersion: tls.VersionTLS12},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	_ = conn.Close()
}
