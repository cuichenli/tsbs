package druid

import (
	"encoding/json"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
	"time"
)

type BaseGenerator struct {
}

func (g *BaseGenerator) fillInQuery(qi query.Query, label string, desc string, sql string) {
	q := qi.(*query.HTTP)
	q.HumanLabel = []byte(label)
	q.RawQuery = []byte(sql)
	q.HumanDescription = []byte(desc)
	q.Method = []byte("POST")
	q.Path = []byte("/druid/v2/sql/")
	bodyMap := make(map[string]string, 1)
	bodyMap["query"] = sql
	body, err := json.Marshal(bodyMap)
	if err != nil {
		panic(err)
	}
	q.Body = body
}

// NewDevops creates a new devops use case query generator.
func (g *BaseGenerator) NewDevops(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := devops.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	devops := &Devops{
		BaseGenerator: g,
		Core:          core,
	}

	return devops, nil
}

// NewIoT creates a new iot use case query generator.
func (g *BaseGenerator) NewIoT(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := iot.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	devops := &IoT{
		BaseGenerator: g,
		Core:          core,
	}

	return devops, nil
}
