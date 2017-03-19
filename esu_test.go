package esu

import (
	"io"
	"os"
	"testing"

	_ "github.com/joho/godotenv/autoload"
)

const (
	DefaultHost = "localhost"
	DefaultPort = "9200"
)

var (
	DefaultOutputWriter io.Writer = os.Stdout
	DefaultErrorWriter  io.Writer = os.Stderr
)

func TestUtils_getConnectionURL(t *testing.T) {
	protocol := EnvGetWithDefault("ES_PROTOCOL", "http")
	host := EnvGetWithDefault("ES_HOST", "localhost")
	port := EnvGetWithDefault("ES_PORT", "9200")

	connection := New(protocol, host, port)

	connection.Ping()

	connection.getClusterHealth()

}
