package druid

import (
	"encoding/json"
	"github.com/timescale/tsbs/pkg/data"
	"io"
)

type Serializer struct{}

func (s *Serializer) Serialize(p *data.Point, w io.Writer) error {
	targetJson := make(map[string]interface{})
	tagValues := p.TagValues()
	tagKeys := p.TagKeys()
	for i := 0; i < len(tagKeys); i++ {
		if tagValues[i] == nil {
			continue
		}
		targetJson[string(tagKeys[i])] = tagValues[i]
	}

	fieldKeys := p.FieldKeys()
	fieldValues := p.FieldValues()

	for i := 0; i < len(fieldKeys); i++ {
		if fieldValues[i] == nil {
			continue
		}
		targetJson[string(fieldKeys[i])] = fieldValues[i]
	}

	timestamp := p.TimestampInUnixMs()
	targetJson["timestamp"] = timestamp
	bytes, err := json.Marshal(targetJson)
	if err != nil {
		return err
	}
	bytes = append(bytes, '\n')
	_, err = w.Write(bytes)
	return err
}
