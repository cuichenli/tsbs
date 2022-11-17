package druid

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
)

func NewTarget() targets.ImplementedTarget {
	return &Target{}
}

type Target struct {
}

func (t *Target) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	//flagSet.String(flagPrefix+"urls", "http://localhost:8086", "InfluxDB URLs, comma-separated. Will be used in a round-robin fashion.")
	//flagSet.Int(flagPrefix+"replication-factor", 1, "Cluster replication factor (only applies to clustered databases).")
	//flagSet.String(flagPrefix+"consistency", "all", "Write consistency. Must be one of: any, one, quorum, all.")
	//flagSet.Duration(flagPrefix+"backoff", time.Second, "Time to sleep between requests when server indicates backpressure is needed.")
	//flagSet.Bool(flagPrefix+"gzip", true, "Whether to gzip encode requests (default true).")
}

func (t *Target) TargetName() string {
	return constants.FormatDruid
}

func (t *Target) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *Target) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("not implemented")
}
