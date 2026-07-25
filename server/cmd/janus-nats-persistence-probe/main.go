package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	natsURL := getenv("JANUS_NATS_URL", "nats://localhost:4222")
	probeSubject := getenv("JANUS_NATS_PROBE_SUBJECT", "janus.probe.persistence")
	streamName := getenv("JANUS_NATS_PROBE_STREAM", "JANUS_PROBE_PERSISTENCE")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nc, err := nats.Connect(natsURL,
		nats.Timeout(5*time.Second),
		nats.ReconnectWait(2*time.Second),
		nats.MaxReconnects(3),
	)
	if err != nil {
		log.Fatalf("failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("failed to create jetstream context: %v", err)
	}

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{probeSubject},
		Retention: jetstream.LimitsPolicy,
		MaxAge:   1 * time.Hour,
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		log.Fatalf("failed to create probe stream: %v", err)
	}
	defer js.DeleteStream(ctx, streamName)

	testMsg := fmt.Sprintf("probe-%d", time.Now().UnixNano())
	_, err = js.Publish(ctx, probeSubject, []byte(testMsg))
	if err != nil {
		log.Fatalf("failed to publish probe message: %v", err)
	}

	cons, err := js.CreateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:       "probe-consumer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		log.Fatalf("failed to create probe consumer: %v", err)
	}
	defer js.DeleteConsumer(ctx, streamName, "probe-consumer")

	batch, err := cons.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		log.Fatalf("failed to fetch probe message: %v", err)
	}

	msgs := batch.Messages()
	msg := <-msgs
	if msg == nil {
		log.Fatalf("no probe message received")
	}
	if string(msg.Data()) != testMsg {
		log.Fatalf("probe message mismatch: got %q, want %q", string(msg.Data()), testMsg)
	}
	msg.Ack()
	log.Printf("NATS JetStream persistence verified: message written and read back successfully")
}

func getenv(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}
