package kafka

import (
	"context"
	k "github.com/segmentio/kafka-go"
	"github.com/timescale/tsbs/pkg/targets"
	"log"
	"math/rand"
)

type processor struct {
	brokers     []string
	topics      []string
	random      *rand.Rand
	authToken   string
	kafkaWriter *k.Writer
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batch := b.(*batch)
	if !doLoad {
		return batch.metrics, batch.rows
	}
	mc, rc := p.do(batch)
	return mc, rc
}

func (p *processor) Init(workerNum int, doLoad, hashWorkers bool) {
	p.kafkaWriter = &k.Writer{
		Addr:     k.TCP(p.brokers...),
		Balancer: &k.LeastBytes{},
	}
}

func (p *processor) do(b *batch) (uint64, uint64) {
	messages := make([]k.Message, b.Len())
	for i, event := range b.events {
		r := p.random.Intn(len(p.topics))
		messages[i] = k.Message{
			Topic: p.topics[r],
			Value: []byte(event),
		}
	}

	err := p.kafkaWriter.WriteMessages(
		context.Background(),
		messages...,
	)
	if err != nil {
		log.Fatalf("error when sending message to kafka: %s", err)
		return 0, 0
	}
	return b.metrics, b.rows
}
