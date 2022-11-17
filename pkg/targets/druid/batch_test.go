package druid

import (
	"bytes"
	"github.com/timescale/tsbs/pkg/data"
	"sync"
	"testing"
)

func TestBatch(t *testing.T) {
	f := &factory{bufPool: &sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 16*1024*1024))
		},
	}}
	b := f.New().(*batch)
	if b.Len() != 0 {
		t.Errorf("batch not initialized with count 0")
	}
	p := data.LoadedPoint{
		Data: []byte("{\"arch\":\"x86\",\"datacenter\":\"eu-central-1a\",\"hostname\":\"host_0\",\"os\":\"Ubuntu15.10\",\"rack\":\"6\",\"region\":\"eu-central-1\",\"service\":\"19\",\"service_environment\":\"test\",\"service_version\":\"1\",\"team\":\"SF\",\"timestamp\":1627776000000,\"usage_guest\":80,\"usage_guest_nice\":38,\"usage_idle\":24,\"usage_iowait\":22,\"usage_irq\":63,\"usage_nice\":61,\"usage_softirq\":6,\"usage_steal\":44,\"usage_system\":2,\"usage_user\":58}"),
	}
	b.Append(p)
	if b.Len() != 1 {
		t.Errorf("batch count is not 1 after first append")
	}
	if b.rows != 1 {
		t.Errorf("batch row count is not 1 after first append")
	}
	if b.metrics != 21 {
		t.Errorf("batch metric count is not 2 after first append")
	}
}
