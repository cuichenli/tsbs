package kafka

import (
	"fmt"
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"github.com/timescale/tsbs/pkg/targets/json"
	"time"
)

func NewTarget() targets.ImplementedTarget {
	return &Target{}
}

type Target struct {
}

const defaultTimeStart = "2016-01-01T00:00:00Z"

const defaultTimeEnd = "2016-01-02T00:00:00Z"
const defaultLogInterval = 10 * time.Second

func (t *Target) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"brokers", "https://localhost:9200", "Kafka broker urls.")
	flagSet.String(flagPrefix+"topics", "index-1", "A list of indexes to be ingested.")
	flagSet.Bool(flagPrefix+"realtime", false, "If should use realtime generated data.")
	flagSet.String("use-case", common.UseCaseCPUOnly, fmt.Sprintf("Use case to generate."))

	flagSet.Uint64("scale", 1, "Scaling value specific to use case (e.g., devices in 'devops').")
	flagSet.Duration("log-interval", defaultLogInterval, "Duration between data points")

	flagSet.String("timestamp-start", defaultTimeStart, "Beginning timestamp (RFC3339).")
	flagSet.String("timestamp-end", defaultTimeEnd, "Ending timestamp (RFC3339).")

	flagSet.Int("debug", 0, "Control level of debug output")
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
