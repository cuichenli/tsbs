package druid

import (
	"bytes"
	"github.com/timescale/tsbs/pkg/data"
)

type batch struct {
	buf     *bytes.Buffer
	rows    uint64
	metrics uint64
}

func (b *batch) Len() uint {
	return uint(b.rows)
}

func (b *batch) Append(point data.LoadedPoint) {
	b.rows++
	dataToWrite := point.Data.([]byte)
	b.metrics += uint64(bytes.Count(dataToWrite, []byte(":")))
	b.buf.Write(point.Data.([]byte))
}
