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
	flagSet.String(flagPrefix+"urls", "http://localhost:8086", "Druid URLs, comma-separated. Will be used in a round-robin fashion.")
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
