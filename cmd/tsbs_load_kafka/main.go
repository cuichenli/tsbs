// tsbs_load_elasticsearch loads a Elasticsearch with data from stdin or file.
package main

import (
	"fmt"
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/kafka"
	"log"
	"strings"
)

// Parse args:
func initProgramOptions() (*kafka.SpecificConfig, load.BenchmarkRunner, *load.BenchmarkRunnerConfig, *common.DataGeneratorConfig) {
	target := kafka.NewTarget()

	loaderConf := load.BenchmarkRunnerConfig{}
	dataGeneratorConfig := common.DataGeneratorConfig{}
	loaderConf.AddToFlagSet(pflag.CommandLine)
	target.TargetSpecificFlags("", pflag.CommandLine)
	pflag.Parse()

	if err := utils.SetupConfigFile(); err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}
	if err := viper.Unmarshal(&loaderConf); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	url := viper.GetString("brokers")
	if len(url) == 0 {
		log.Fatalf("missing `brokers` flag")
	}
	urls := strings.Split(url, ",")
	topics := viper.GetString("topics")
	if len(topics) == 0 {
		log.Fatalf("missing `topics` flag")
	}
	realtime := viper.GetBool("realtime")

	if realtime {
		if err := viper.Unmarshal(&dataGeneratorConfig); err != nil {
			panic(fmt.Errorf("unalbe to encode config for data generator: %s", err))
		}
		if err := viper.Unmarshal(&dataGeneratorConfig.BaseConfig); err != nil {
			panic(fmt.Errorf("unalbe to encode config for data generator base config: %s", err))
		}
		dataGeneratorConfig.Format = "json"
		dataGeneratorConfig.InterleavedNumGroups = 1
		dataGeneratorConfig.File = ""
	}
	loader := load.GetBenchmarkRunner(loaderConf)
	return &kafka.SpecificConfig{
		BrokerUrls: urls,
		Topics:     strings.Split(topics, ","),
		Realtime:   realtime,
	}, loader, &loaderConf, &dataGeneratorConfig
}

func main() {
	kafkaConf, loader, loaderConf, dataGeneratorConf := initProgramOptions()
	var benchmark targets.Benchmark
	var err error
	if kafkaConf.Realtime {
		benchmark, err = kafka.NewBenchmark(kafkaConf, &source.DataSourceConfig{
			Type:      source.SimulatorDataSourceType,
			Simulator: dataGeneratorConf,
		})
	} else {
		benchmark, err = kafka.NewBenchmark(kafkaConf, &source.DataSourceConfig{
			Type: source.FileDataSourceType,
			File: &source.FileDataSourceConfig{Location: loaderConf.FileName},
		})
	}
	if err != nil {
		panic(err)
	}
	loader.RunBenchmark(benchmark)
}
