package kafka

import (
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"net"
	"strconv"
)

// Druid don't have a database abstraction
type dbCreator struct {
	topics        []string
	brokers       []string
	conneciton    *kafka.Conn
	missingTopics []string
}

func (d *dbCreator) Init() {
	var conn *kafka.Conn
	var err error
	for _, broker := range d.brokers {
		conn, err = kafka.Dial("tcp", broker)
		if err != nil {
			log.Fatalf("failed to connect to broker %s: %s", d.brokers[0], err)
		} else {
			break
		}
	}
	if conn == nil {
		panic("none of the brokers is accessible")
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		panic(fmt.Sprintf("failed to resolve the controller of the kafka cluster: %s", err))
	}
	var connLeader *kafka.Conn
	connLeader, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	d.conneciton = connLeader
}

func (d *dbCreator) DBExists(dbName string) bool {
	partitions, err := d.conneciton.ReadPartitions()
	if err != nil {
		panic(err.Error())
	}
	record := make(map[string]string, 0)
	for _, p := range partitions {
		record[p.Topic] = ""
	}
	for _, topic := range d.topics {
		if _, ok := record[topic]; !ok {
			d.missingTopics = append(d.missingTopics, topic)
		}
	}
	return len(d.missingTopics) == 0
}

func (d *dbCreator) CreateDB(dbName string) error {
	topics := make([]kafka.TopicConfig, len(d.missingTopics))
	for i, topic := range d.missingTopics {
		topics[i] = kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     -1,
			ReplicationFactor: -1,
		}
	}
	err := d.conneciton.CreateTopics(topics...)
	return err
}

func (d *dbCreator) RemoveOldDB(dbName string) error { return nil }

func (d *dbCreator) Close() {
	if d.conneciton != nil {
		d.conneciton.Close()
	}
}
