package esu

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	logger "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	context "golang.org/x/net/context"
	elastic "gopkg.in/olivere/elastic.v5"
)

//go:generate sh -c "mockery -name='IndexManager' -case=underscore"

// var logger = logging.MustGetLogger("search.elasticsearch.indexes")

// CreateFlags are flags you can pass to IndexManager's Create method.
type CreateFlags struct {
	// Temporary sets whether to make this index a temporary one. It will optimize for writing,
	// by disabling replication, disabling refresh, and disabling translog durability.
	Temporary bool
}

// IndexManager manages indexes.
type IndexManager interface {
	// Create creates a new index. If temporary is true, the index is created
	// with less durable settings.
	Create(indexName string, flags CreateFlags, mappings interface{}) error

	// Delete deletes an index.
	Delete(indexName string) error

	// MakePermanent transitions a temporary index to a permanent one.
	MakePermanent(indexName string) error

	// GetNames returns the names of all existing indexes.
	GetNames() ([]string, error)

	// IndexExists checks if the index exists.
	IndexExists(indexName string) (bool, error)
}

type indexManager struct {
	client        *elastic.Client
	indexSettings jsonMap
	esVersion     ESVersion
}

func NewIndexManager(
	client *elastic.Client,
	indexSettings *json.RawMessage) (IndexManager, error) {
	esVersion, err := DetectVersion(client)
	if err != nil {
		return nil, err
	}

	var settings jsonMap
	if indexSettings != nil {
		// TODO: We need to unflatten it so that we can modify it consistently
		settings = jsonMap{}
		if err := json.Unmarshal(*indexSettings, &settings); err != nil {
			return nil, errors.Wrap(err, "Invalid index settings JSON")
		}
	}

	return &indexManager{
		client:        client,
		indexSettings: settings,
		esVersion:     esVersion,
	}, nil
}

func (mgr *indexManager) Create(
	indexName string,
	flags CreateFlags,
	mappings interface{}) error {
	ctx := context.Background()

	logger.Infof("Creating index %q", indexName)

	settings := mgr.indexSettings.copy()
	if flags.Temporary {
		settings["number_of_replicas"] = 0
		settings["refresh_interval"] = -1
		settings["translog"] = jsonMap{"durability": "async"}
	}

	_, err := mgr.client.CreateIndex(indexName).
		BodyJson(jsonMap{
			"settings": jsonMap{
				"index": settings,
			},
			"mappings": mappings,
		}).Do(ctx)
	if err != nil {
		logger.Errorf("Could not create index %q, escalating: %s", indexName, err)
		return errors.Wrapf(err, "Unable to create index %q", indexName)
	}

	logger.Infof("Waiting for newly created index %q", indexName)

	_, err = mgr.client.ClusterHealth().
		Timeout("30s").
		Index(indexName).
		WaitForYellowStatus().Do(ctx)
	if err != nil {
		return errors.Wrapf(err,
			"Created index %q, but timed out waiting for cluster status to turn yellow or green",
			indexName)
	}

	logger.Infof("Created index %q", indexName)
	return nil
}

func (mgr *indexManager) Delete(indexName string) error {
	logger.Infof("Deleting index %q", indexName)

	_, err := mgr.client.DeleteIndex(indexName).Do(context.Background())
	if err != nil {
		if IsElasticErrorOfType(err, "index_not_found_exception") {
			logger.Infof("Index %q did not exist", indexName)
			return nil
		}
		return errors.Wrapf(err, "Failed to delete index %q", indexName)
	}

	logger.Infof("Deleted index %q", indexName)
	return nil
}

func (mgr *indexManager) GetNames() ([]string, error) {
	// resp, err := mgr.client.IndexGet("*").AllowNoIndexes(true).Do(context.Background())
	resp, err := mgr.client.IndexGet().Index("*").IgnoreUnavailable(true).Do(context.Background())
	if err != nil {
		return nil, errors.Wrap(err, "Could not get indexes")
	}

	names := make([]string, len(resp))
	i := 0
	for name := range resp {
		names[i] = name
		i++
	}
	return names, nil
}

func (mgr *indexManager) MakePermanent(indexName string) error {
	ctx := context.Background()

	logger.Infof("Finalizing settings of index %q", indexName)

	if _, err := mgr.client.IndexPutSettings(indexName).
		BodyJson(jsonMap{
			"index": mgr.getPermanentIndexSettings(),
		}).Do(ctx); err != nil {
		return err
	}

	logger.Infof("Flushing index %q", indexName)
	if _, err := mgr.client.Flush(indexName).IgnoreUnavailable(true).Do(ctx); err != nil {
		logger.Warningf("Unable to flush index %q, ignoring: %s", err)
	}

	return nil
}

func (mgr *indexManager) IndexExists(indexName string) (bool, error) {
	ctx := context.Background()
	return mgr.client.IndexExists(indexName).Do(ctx)
}

func (mgr *indexManager) getPermanentIndexSettings() jsonMap {
	settings := jsonMap{}

	// For all versions of ES, there's no way to find the default. For ES >= 5.0, this
	// is not a problem, as the global index settings are removed, and the default is
	// always the same. For < 5.0, it might be wrong.
	settings["number_of_replicas"] = 5

	if mgr.esVersion[0] >= 5 {
		// For ES >= 5.0, we can simply delete the temporary settings
		settings["refresh_interval"] = nil
		settings["translog"] = jsonMap{"durability": nil}
	} else {
		// For ES < 5.0, we can't remove settings, so we have to set the defaults.
		// These are the defaults that ES currently uses for new indexes, if the
		// global configuration hasn't overridden them.
		settings["refresh_interval"] = "1s"
		settings["translog"] = jsonMap{"durability": "request"}
	}
	settings = settings.merge(mgr.indexSettings)

	// ES will complain if we try to set the number of shards
	delete(settings, "number_of_shards")

	return settings
}

type ESVersion []int

func DetectVersion(client *elastic.Client) (ESVersion, error) {
	resp, err := client.NodesInfo().NodeId("_local").Do(context.Background())
	if err != nil {
		return nil, errors.Wrap(err, "Error while detecting Elasticsearch version")
	}
	for _, node := range resp.Nodes {
		return parseVersion(node.Version), nil
	}
	panic("Elasticsearch node list unexpectedly empty")
}

func parseVersion(s string) (version ESVersion) {
	parts := strings.Split(s, ".")
	version = make(ESVersion, len(parts))
	for i, part := range parts {
		n, err := strconv.ParseInt(part, 10, 32)
		if err != nil {
			panic(fmt.Sprintf("ES returned invalid version: %q", s))
		}
		version[i] = int(n)
	}
	return
}

func IsElasticErrorOfType(err error, exceptionType string) bool {
	if err == nil {
		return false
	}

	if e, ok := err.(*elastic.Error); ok {
		return e.Details != nil && e.Details.Type == exceptionType
	}
	return false
}
