package esu

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"

	elastic "gopkg.in/olivere/elastic.v5"

	"github.com/fatih/color"
)

func getConnectionURL(scheme, host, port string) *url.URL {

	return &url.URL{
		Scheme: scheme,
		Host:   fmt.Sprintf("%s:%s", host, port),
	}
}

func connectToES(urls []*url.URL) (es *elastic.Client) {
	var uris []string
	for _, u := range urls {
		uris = append(uris, u.String())
	}
	es, err := elastic.NewClient(
		elastic.SetURL(uris...),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
	)

	if err != nil {
		exitWithError(err)
	}

	return es
}

func getStdIn() io.Reader {
	info, err := os.Stdin.Stat()

	if err != nil {
		return nil
	}

	if info.Size() == 0 {
		return nil
	}

	return os.Stdin
}

func getFile(path string) io.Reader {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	return f
}

func readJSON(r io.Reader) (out map[string]interface{}, err error) {
	d := json.NewDecoder(r)
	err = d.Decode(&out)
	return
}

func exitWithError(err error) {
	txt := color.New(color.FgRed).SprintfFunc()("\nERROR: %v", err)
	fmt.Fprintln(os.Stderr, txt)
	os.Exit(1)
}

// EnvGetWithDefault gets environment variable, default value returned if it do not exist
func EnvGetWithDefault(envVar, value string) string {
	v1 := os.Getenv(envVar)
	if v1 != "" {
		return v1
	}
	return value
}

// EnvGetIntWithDefault gets environment variable and converts it to int, default value returned if it do not exist
func EnvGetIntWithDefault(envVar string, value int) int {
	v1 := os.Getenv(envVar)
	if v1 != "" {
		v, err := strconv.ParseInt(v1, 10, 0)
		if err == nil {
			return int(v)
		}
	}
	return value
}
