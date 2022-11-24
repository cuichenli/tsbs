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
	"github.com/timescale/tsbs/pkg/targets/elasticsearch"
	"log"
	"strings"
)

// Parse args:
func initProgramOptions() (*elasticsearch.SpecificConfig, load.BenchmarkRunner, *load.BenchmarkRunnerConfig, *common.DataGeneratorConfig) {
	target := elasticsearch.NewTarget()

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

	url := viper.GetString("url")
	if len(url) == 0 {
		log.Fatalf("missing `url` flag")
	}
	indexes := viper.GetString("indexes")
	if len(indexes) == 0 {
		log.Fatalf("missing `indexes` flag")
	}
	realtime := viper.GetBool("realtime")
	username := viper.GetString("username")
	password := viper.GetString("password")

	if realtime {
		if err := viper.Unmarshal(&dataGeneratorConfig); err != nil {
			panic(fmt.Errorf("unalbe to encode config for data generator: %s", err))
		}
		if err := viper.Unmarshal(&dataGeneratorConfig.BaseConfig); err != nil {
			panic(fmt.Errorf("unalbe to encode config for data generator base config: %s", err))
		}
		dataGeneratorConfig.Format = "elasticsearch"
		dataGeneratorConfig.InterleavedNumGroups = 1
		dataGeneratorConfig.File = ""
	}
	loader := load.GetBenchmarkRunner(loaderConf)
	return &elasticsearch.SpecificConfig{
		ServerURL: url,
		Indexes:   strings.Split(indexes, ","),
		Realtime:  realtime,
		Username:  username,
		Password:  password,
	}, loader, &loaderConf, &dataGeneratorConfig
}

func main() {
	esconf, loader, loaderConf, dataGeneratorConf := initProgramOptions()
	var benchmark targets.Benchmark
	var err error
	if esconf.Realtime {
		benchmark, err = elasticsearch.NewBenchmark(esconf, &source.DataSourceConfig{
			Type:      source.SimulatorDataSourceType,
			Simulator: dataGeneratorConf,
		})
	} else {
		benchmark, err = elasticsearch.NewBenchmark(esconf, &source.DataSourceConfig{
			Type: source.FileDataSourceType,
			File: &source.FileDataSourceConfig{Location: loaderConf.FileName},
		})
	}
	if err != nil {
		panic(err)
	}
	loader.RunBenchmark(benchmark)
}
