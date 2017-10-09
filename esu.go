package esu

import (
	"net/url"
	"strings"

	elastic "gopkg.in/olivere/elastic.v5"
)

// EsConnection is a container for elasticsearch connection information
type EsConnection struct {
	Scheme string
	Host   string
	Port   string
	URLs   []*url.URL
	Client *elastic.Client
}

// New Creates a  ES connection object
func New(scheme, host, port string) *EsConnection {
	connection := EsConnection{Scheme: scheme, Host: host, Port: port}
	connection.URLs = append(connection.URLs, getConnectionURL(scheme, host, port))
	connection.Client = connectToES(connection.URLs)

	return &connection
}

// NewByUrl Creates a  ES connection object based on elastic url
func NewByUrl(url string) *EsConnection {
	return NewByUrls(url)
}

/* NewByUrls creates an ES connection based on a list of comma separated
urls. */
func NewByUrls(urls string) *EsConnection {
	connection := EsConnection{}
	for _, u := range strings.Split(urls, ",") {
		parsedUrl, err := url.Parse(u)
		if err != nil {
			panic(err)
		}
		connection.URLs = append(connection.URLs, parsedUrl)
	}
	connection.Client = connectToES(connection.URLs)
	return &connection
}
