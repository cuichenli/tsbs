package druid

import (
	"fmt"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/pkg/query"
	"time"
)

type IoT struct {
	*iot.Core
	*BaseGenerator
}

func (i *IoT) DailyTruckActivity(q query.Query) {
	druidql := fmt.Sprintf(`
SELECT count("ms")/144.0 as ms, model, fleet from (
  SELECT avg(cast("status" as float)) as ms, model, fleet, TIME_FLOOR("__time", 'PT10M') as t
	 FROM "iot" 
	 WHERE TIME_IN_INTERVAL(__time, '%s')
 GROUP BY TIME_FLOOR("__time", 'PT10M'), "model", "fleet"
 )
WHERE ms < 1
GROUP BY FLOOR("t" TO DAY), "model", "fleet"
`, i.GetInterval())

	humanLabel := "Druid daily truck activity per fleet per model"
	humanDesc := humanLabel

	i.fillInQuery(q, humanLabel, humanDesc, druidql)
}

func (i *IoT) GenerateEmptyQuery() query.Query {
	return query.NewHTTP()
}

func (i *IoT) TrucksWithLowFuel(qi query.Query) {
	druidql := fmt.Sprintf(`{
	  "queryType": "groupBy",
	  "dataSource": "iot",
	  "granularity": "second",
	  "dimensions": ["name", "driver", "fuel_state"],
	  "filter": {
		"type": "and", 
		"fields": [{
		  "type": "bound",
		  "dimension": "fuel_state",
		  "upper": "0.1" ,
		  "ordering": "numeric"
		},
		{
		  "type": "selector", "dimension": "fleet", "value": "%s"
		}]
	  },
	  "aggregations": [{
		"type" : "doubleFirst",
		"name" : "__t",
		"fieldName" : "__time"
	  }],
	  "intervals": [ "%s" ]
	}`,
		i.GetRandomFleet(),
		i.GetInterval(),
	)

	humanLabel := "druid trucks with low fuel"
	humanDesc := fmt.Sprintf("%s: under 10 percent", humanLabel)

	i.fillInNativeQuery(qi, humanLabel, humanDesc, druidql)
}

func (i *IoT) GetInterval() string {
	interval := i.Interval.StaticWindow(12 * time.Hour)
	return fmt.Sprintf("%s/%s", interval.Start().UTC().Format(time.RFC3339), interval.End().UTC().Format(time.RFC3339))
}
