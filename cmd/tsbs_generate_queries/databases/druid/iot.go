package druid

import (
	"fmt"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/pkg/query"
)

type IoT struct {
	*iot.Core
	*BaseGenerator
}

func (i IoT) GenerateEmptyQuery() query.Query {
	return query.NewHTTP()
}

func (i *IoT) TrucksWithLowFuel(qi query.Query) {
	influxql := fmt.Sprintf(`SELECT "name", "driver", "fuel_state" 
		FROM "iot" 
		WHERE "fuel_state" <= 0.1 AND "fleet" = '%s' 
		GROUP BY "name" 
		ORDER BY "time" DESC 
		LIMIT 1`,
		i.GetRandomFleet())

	humanLabel := "Influx trucks with low fuel"
	humanDesc := fmt.Sprintf("%s: under 10 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, influxql)
}
