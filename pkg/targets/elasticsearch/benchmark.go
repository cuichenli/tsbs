package elasticsearch

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
	ServerURL string
	Indexes   []string `yaml:"indexes" mapstructure:"indexes"`
	Realtime  bool     `yaml:"realtime" mapstructure:"realtime"`
}

type benchmark struct {
	serverURL  string
	indexes    []string
	dataSource targets.DataSource
}

func NewBenchmark(esSpecificConfig *SpecificConfig, dataSourceConfig *source.DataSourceConfig) (targets.Benchmark, error) {
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
			serverURL: esSpecificConfig.ServerURL,
			indexes:   esSpecificConfig.Indexes,
		}, nil
	} else {
		br := load.GetBufferedReader(dataSourceConfig.File.Location)
		return &benchmark{
			dataSource: &fileDataSource{
				scanner: bufio.NewScanner(br),
			},
			serverURL: esSpecificConfig.ServerURL,
			indexes:   esSpecificConfig.Indexes,
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
	return &processor{url: b.serverURL + "/_bulk", indexes: b.indexes, random: rand.New(rand.NewSource(time.Now().UnixNano()))}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{}
}

type factory struct {
	bufPool *sync.Pool
}

func (f *factory) New() targets.Batch {
	return &batch{buf: f.bufPool.Get().(*bytes.Buffer)}
}
