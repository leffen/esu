package esu

import (
	"context"

	log "github.com/Sirupsen/logrus"

	elastic "gopkg.in/olivere/elastic.v5"
)

// PumpData is the way to communicate between channels
type PumpData struct {
	IsEOF bool
	Rec   interface{}
	UID   string
	JSON  string
}

// Datapump - Wrapping structure for connection and index information
type Datapump struct {
	Connection  *EsConnection
	Index       string
	IndexType   string
	BulkActions int
	BulkSize    int
	BulkWorkers int
}

// NewDatapump - Creates a new datapump
func NewDatapump(cn *EsConnection, index, indexType string, bulkActions, bulkSize, bulkWorkers int) *Datapump {
	pmp := Datapump{
		Connection:  cn,
		Index:       index,
		IndexType:   indexType,
		BulkActions: bulkActions,
		BulkSize:    bulkSize,
		BulkWorkers: bulkWorkers,
	}

	return &pmp
}

// Listen for data to send to elastic
func (pump *Datapump) Listen(lc chan PumpData, ec chan int) {

	ctx := context.Background()
	client := pump.Connection.Client

	log.Debug("Datapump.listen index= ", pump.Index, " index type= ", pump.IndexType)

	var indices []string
	indices = append(indices, pump.Index)

	exists, err := elastic.NewIndicesExistsService(client).Index(indices).Do(ctx)
	if err != nil {
		log.Fatal(err)
	}

	if !exists {
		_, err := client.CreateIndex(pump.Index).Do(ctx)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("created index %s\n", pump.Index)
	}

	pump.setRefreshInterval(pump.Index, "-1")

	rows := 0

	p, err := client.BulkProcessor().Name("Eliot importer").BulkActions(pump.BulkActions).BulkSize(1000000000).Workers(pump.BulkWorkers).Stats(true).Do(ctx)
	if err != nil {
		log.Fatal(err)
	}

	for {
		data := <-lc
		if data.IsEOF {
			break
		}
		req := elastic.NewBulkIndexRequest().Index(pump.Index).Type(pump.IndexType).Id(data.UID).Doc(data.JSON)
		p.Add(req)
		if err != nil {
			log.Fatal(err)
		}
		rows++
		if rows%100000 == 0 {
			log.Debugln("Datapump", rows)
		}

	}

	err = p.Flush()
	if err != nil {
		log.Fatal(err)
	}

	pump.setRefreshInterval(pump.Index, "1s")

	printBulkStats(p)

	p.Close()
	ec <- 1
}

func printBulkStats(p *elastic.BulkProcessor) {
	stats := p.Stats()

	log.Debugf("Number of times flush has been invoked: %d\n", stats.Flushed)
	log.Debugf("Number of times workers committed reqs: %d\n", stats.Committed)
	log.Debugf("Number of requests indexed            : %d\n", stats.Indexed)
	log.Debugf("Number of requests reported as created: %d\n", stats.Created)
	log.Debugf("Number of requests reported as updated: %d\n", stats.Updated)
	log.Debugf("Number of requests reported as success: %d\n", stats.Succeeded)
	log.Debugf("Number of requests reported as failed : %d\n", stats.Failed)

	for i, w := range stats.Workers {
		log.Debugf("Worker %d: Number of requests queued: %d\n", i, w.Queued)
		log.Debugf("           Last response time       : %v\n", w.LastDuration)
	}
}

func (pump *Datapump) setRefreshInterval(index, interval string) {

	body := `{"index":{"refresh_interval":"` + interval + `"}}`

	// Put settings
	putres, err := pump.Connection.Client.IndexPutSettings().Index(index).BodyString(body).Do(context.TODO())
	if err != nil {
		log.Fatalf("expected put settings to succeed; got: %v", err)
	}
	if putres == nil {
		log.Fatalf("expected put settings response; got: %v", putres)
	}
	if !putres.Acknowledged {
		log.Fatalf("expected put settings ack; got: %v", putres.Acknowledged)
	}

	log.Debug("Updated index with new refresh refresh_interval")

}
