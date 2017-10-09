package esu

import (
	"context"
	"fmt"
)

func (cn *EsConnection) Ping() {
	ctxb := context.Background()
	uri := cn.URLs[0].String()
	res, _, err := cn.Client.Ping(uri).Do(ctxb)

	if err != nil {
		exitWithError(err)
	}

	t := NewTable("Cluster", res.ClusterName)
	t.Add("Node", fmt.Sprintf("%s [%v]", res.Name, uri))
	t.Add("Tag Line", res.TagLine)
	t.Add("ES Version", res.Version.Number)
	t.Print()
}
