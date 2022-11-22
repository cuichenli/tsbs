package elasticsearch

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"github.com/timescale/tsbs/pkg/targets/json"
)

func NewTarget() targets.ImplementedTarget {
	return &Target{}
}

type Target struct {
}

func (t *Target) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"url", "http://localhost:8080", "ES URL")
	flagSet.String(flagPrefix+"indexes", "index-1", "A list of indexes to be ingested.")
}

func (t *Target) TargetName() string {
	return constants.FormatDruid
}

func (t *Target) Serializer() serialize.PointSerializer {
	return &json.Serializer{}
}

func (t *Target) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("not implemented")
}
