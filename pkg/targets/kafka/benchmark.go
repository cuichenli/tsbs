package kafka

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/timescale/tsbs/internal/inputs"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"math/rand"
	"sync"
	"time"
)

type SpecificConfig struct {
	BrokerUrls []string
	Topics     []string `yaml:"topics" mapstructure:"topics"`
	Realtime   bool     `yaml:"realtime" mapstructure:"realtime"`
}

type benchmark struct {
	brokerUrls []string
	topics     []string
	dataSource targets.DataSource
	authToken  string
}

func NewBenchmark(kafkaSpecificConfig *SpecificConfig, dataSourceConfig *source.DataSourceConfig) (targets.Benchmark, error) {
	var authToken string
	if dataSourceConfig.Type != source.FileDataSourceType {
		var dummyBuffer bytes.Buffer
		dataGenerator := &inputs.DataGenerator{
			Out: bufio.NewWriter(&dummyBuffer),
		}
		simulator, err := dataGenerator.CreateSimulator(dataSourceConfig.Simulator)
		if err != nil {
			panic(fmt.Errorf("failed to create simulator: %s", err))
		}
		return &benchmark{
			dataSource: &realtimeDataSource{
				simulator,
			},
			brokerUrls: kafkaSpecificConfig.BrokerUrls,
			topics:     kafkaSpecificConfig.Topics,
			authToken:  authToken,
		}, nil
	} else {
		br := load.GetBufferedReader(dataSourceConfig.File.Location)
		return &benchmark{
			dataSource: &fileDataSource{
				scanner: bufio.NewScanner(br),
			},
			brokerUrls: kafkaSpecificConfig.BrokerUrls,
			topics:     kafkaSpecificConfig.Topics,
			authToken:  authToken,
		}, nil
	}
}

func (b *benchmark) GetDataSource() targets.DataSource {
	return b.dataSource
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	bufPool := sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 16*1024*1024))
		},
	}
	return &factory{bufPool: &bufPool}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{brokers: b.brokerUrls, topics: b.topics, random: rand.New(rand.NewSource(time.Now().UnixNano())), authToken: b.authToken}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{
		topics:  b.topics,
		brokers: b.brokerUrls,
	}
}

type factory struct {
	bufPool *sync.Pool
}

func (f *factory) New() targets.Batch {
	return &batch{buf: f.bufPool.Get().(*bytes.Buffer)}
}
