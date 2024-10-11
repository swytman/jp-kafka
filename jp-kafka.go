package jpkafka

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "sync"

    "github.com/twmb/franz-go/pkg/kgo"
)

type KafkaWriter struct {
    Client *kgo.Client
    Topic  string
}

type KafkaReader struct {
	Client *kgo.Client
}

// NewKafkaWriter creates a new KafkaWriter using host and port parameters
func NewKafkaWriter(kafkaHost, kafkaPort, topic string) *KafkaWriter {
    seeds := []string{fmt.Sprintf("%s:%s", kafkaHost, kafkaPort)}

    client, err := kgo.NewClient(
        kgo.SeedBrokers(seeds...),
    )
    if err != nil {
        log.Fatalf("Failed to create Kafka client: %v", err)
    }

    return &KafkaWriter{Client: client, Topic: topic}
}

// WriteMessage writes a custom JSON message to Kafka
func (kw *KafkaWriter) WriteMessage(key string, value interface{}) error {
    jsonValue, err := json.Marshal(value)
    if err != nil {
        return fmt.Errorf("failed to marshal message to JSON: %w", err)
    }

    record := &kgo.Record{
        Topic: kw.Topic,
        Key:   []byte(key),
        Value: jsonValue,
    }

    var wg sync.WaitGroup
    wg.Add(1)

    kw.Client.Produce(context.Background(), record, func(_ *kgo.Record, err error) {
        defer wg.Done()
        if err != nil {
            log.Printf("record had a produce error: %v\n", err)
            return
        }
        log.Printf("Produced record with key: %s", key)
    })

    wg.Wait()
    return nil
}

func (kw *KafkaWriter) Close() {
    kw.Client.Close()
}


// NewKafkaReader initializes a new KafkaReader with franz-go.
func NewKafkaReader(topic string) *KafkaReader {
	kafkaHost := os.Getenv("KAFKA_HOST")
	kafkaPort := os.Getenv("KAFKA_PORT")
	brokers := strings.Split(kafkaHost+":"+kafkaPort, ",")

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup("payment-consumers"),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("Failed to create Kafka client: %v", err)
	}

	return &KafkaReader{
		Client: client,
	}
}

// ReadMessages fetches messages from Kafka and commits manually.
func (kr *KafkaReader) ReadMessages(ctx context.Context) ([]*kgo.Record, error) {
	fetches := kr.Client.PollFetches(ctx)

	if fetches.IsClientClosed() {
		return nil, nil
	}

	fetches.EachError(func(t string, p int32, err error) {
		log.Printf("Error fetching from topic %s partition %d: %v", t, p, err)
	})

	var records []*kgo.Record
	fetches.EachRecord(func(record *kgo.Record) {
		records = append(records, record)
	})

	return records, nil
}

// CommitRecords commits the consumed records.
func (kr *KafkaReader) CommitRecords(ctx context.Context, records []*kgo.Record) error {
	if err := kr.Client.CommitRecords(ctx, records...); err != nil {
		log.Printf("Error committing records: %v", err)
		return err
	}
	return nil
}

// Close closes the Kafka reader.
func (kr *KafkaReader) Close() {
	kr.Client.Close()
}

