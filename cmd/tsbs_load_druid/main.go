package main

import (
	"fmt"
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets/druid"
	"log"
	"strings"
)

func initProgramOptions() (*druid.SpecificConfig, load.BenchmarkRunner, *load.BenchmarkRunnerConfig) {
	target := druid.NewTarget()

	loaderConf := load.BenchmarkRunnerConfig{}
	loaderConf.AddToFlagSet(pflag.CommandLine)
	target.TargetSpecificFlags("", pflag.CommandLine)
	pflag.Parse()

	if err := utils.SetupConfigFile(); err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}
	if err := viper.Unmarshal(&loaderConf); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	urls := viper.GetString("urls")
	if len(urls) == 0 {
		log.Fatalf("missing `urls` flag")
	}
	druidUrls := strings.Split(urls, ",")

	loader := load.GetBenchmarkRunner(loaderConf)
	return &druid.SpecificConfig{ServerURLs: druidUrls}, loader, &loaderConf
}

func main() {
	vmConf, loader, loaderConf := initProgramOptions()

	benchmark, err := druid.NewBenchmark(vmConf, &source.DataSourceConfig{
		Type: source.FileDataSourceType,
		File: &source.FileDataSourceConfig{Location: loaderConf.FileName},
	})
	if err != nil {
		panic(err)
	}
	loader.RunBenchmark(benchmark)
}
