package elasticsearch

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"github.com/timescale/tsbs/pkg/targets"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

type processor struct {
	url     string
	indexes []string
	random  *rand.Rand
}

func (p *processor) Init(workerNum int, doLoad, hashWorkers bool) {
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batch := b.(*batch)
	if !doLoad {
		return batch.metrics, batch.rows
	}
	mc, rc := p.do(batch)
	return mc, rc
}

func (p *processor) generateMeta() []byte {
	meta, err := json.Marshal(map[string]interface{}{
		"index": map[string]string{
			"_index": p.indexes[p.random.Intn(len(p.indexes))],
			"_type":  "doc",
		},
	})
	if err != nil {
		panic("Failed to generate random index")
	}
	meta = append(meta, '\n')
	return meta
}

func (p *processor) do(b *batch) (uint64, uint64) {
	for {
		var bodyBuilder strings.Builder
		for _, event := range b.events {
			meta := p.generateMeta()
			bodyBuilder.Write(meta)
			bodyBuilder.WriteString(event)
			bodyBuilder.Write([]byte{'\n'})
		}
		http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}

		r := bytes.NewBufferString(bodyBuilder.String())
		req, err := http.NewRequest("POST", p.url, r)
		if err != nil {
			log.Fatalf("error while creating new request: %s", err)
		}
		req.Header.Set("Content-Type", "plain/text")
		req.Header.Set("Accept", "*/*")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatalf("error while executing request: %s", err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			b.buf.Reset()
			b.events = make([]string, 0)
			return b.metrics, b.rows
		}
		log.Printf("server returned HTTP status %d. Retrying", resp.StatusCode)
		time.Sleep(time.Millisecond * 10)
	}
}
