package esu

import (
	"net/url"

	elastic "gopkg.in/olivere/elastic.v5"
)

const VERSION = "0.2.0"

// EsConnection is a container for elasticsearch connection information
type EsConnection struct {
	Scheme string
	Host   string
	Port   string
	URL    *url.URL
	Client *elastic.Client
}

// New Creates a  ES connection object
func New(scheme, host, port string) *EsConnection {
	connection := EsConnection{Scheme: scheme, Host: host, Port: port}
	connection.URL = getConnectionURL(scheme, host, port)
	connection.Client = connectToES(connection.URL.String())

	return &connection
}

// NewByUrl Creates a  ES connection object based on elastic url
func NewByUrl(url string) *EsConnection {
	connection := EsConnection{}
	connection.Client = connectToES(url)

	return &connection
}
