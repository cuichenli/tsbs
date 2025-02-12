package druid

import (
	"bufio"
	"bytes"
	"errors"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"sync"
)

type SpecificConfig struct {
	ServerURLs []string
}

type benchmark struct {
	serverURLs []string
	dataSource targets.DataSource
}

func NewBenchmark(druidSpecificConfig *SpecificConfig, dataSourceConfig *source.DataSourceConfig) (targets.Benchmark, error) {
	if dataSourceConfig.Type != source.FileDataSourceType {
		return nil, errors.New("only FILE data source type is supported for VictoriaMetrics")
	}

	br := load.GetBufferedReader(dataSourceConfig.File.Location)
	return &benchmark{
		dataSource: &fileDataSource{
			scanner: bufio.NewScanner(br),
		},
		serverURLs: druidSpecificConfig.ServerURLs,
	}, nil
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
	return &processor{vmURLs: b.serverURLs}
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
