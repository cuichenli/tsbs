package kafka

import (
	"bytes"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets/json"
	"time"
)

type realtimeDataSource struct {
	simulator common.Simulator
}

func (r realtimeDataSource) NextItem() data.LoadedPoint {
	newSimulatorPoint := data.NewPoint()
	if !r.simulator.Next(newSimulatorPoint) {
		return data.LoadedPoint{}
	}
	t := time.Now()
	newSimulatorPoint.SetTimestamp(&t)
	serializer := &json.Serializer{}
	var buffer bytes.Buffer
	err := serializer.Serialize(newSimulatorPoint, &buffer)
	if err != nil {
		return data.LoadedPoint{}
	}

	return data.LoadedPoint{Data: buffer.Bytes()}
}

func (r realtimeDataSource) Headers() *common.GeneratedDataHeaders {
	return nil
}
