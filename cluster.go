package esu

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	elastic "gopkg.in/olivere/elastic.v5"

	"github.com/fatih/color"
)

func (cn *EsConnection) getClusterHealth() {
	ctxb := context.Background()

	res, err := cn.Client.ClusterHealth().Do(ctxb)
	if err != nil {
		exitWithError(err)
	}

	c := color.New()
	switch res.Status {
	case "red":
		c.Add(color.FgRed)
	case "green":
		c.Add(color.FgGreen)
	default:
		c.Add(color.FgYellow)
	}

	t := NewTable(res.ClusterName, fmt.Sprint("status: ", res.Status))
	t.HeaderColor = c.Add(color.Underline)

	// Node Info
	t.Add("Total Nodes", res.NumberOfDataNodes)
	t.Add("Data Nodes", res.NumberOfDataNodes)
	t.Add()

	// Shard Info
	t.Add("Active Shards", fmt.Sprintf("%d (%.2f%%)", res.ActiveShards, res.ActiveShardsPercentAsNumber))
	if res.ActivePrimaryShards > 0 {
		t.Add("Primary Shards", res.ActivePrimaryShards)
	}
	if res.RelocatingShards > 0 {
		t.Add("Relocating Shards", res.RelocatingShards)
	}
	if res.InitializingShards > 0 {
		t.Add("Initializing Shards", res.InitializingShards)
	}
	if res.UnassignedShards > 0 || res.DelayedUnassignedShards > 0 {
		t.Add("Unassigned Shards", fmt.Sprintf("%d (%d Delayed)", res.UnassignedShards, res.DelayedUnassignedShards))
	}
	t.Add()

	// Task Info
	t.Add("Pending Tasks", res.NumberOfPendingTasks)
	t.Add("Max Time in Task Queue", fmt.Sprintf("%d ms", res.TaskMaxWaitTimeInQueueInMillis))
	t.Add("In-Flight Fetches", res.NumberOfInFlightFetch)

	t.Print()
}

func (cn *EsConnection) getClusterStats() {
	ctxb := context.Background()
	res, err := cn.Client.ClusterStats().Human(true).Do(ctxb)
	if err != nil {
		exitWithError(err)
	}

	var t *Table
	if res.Indices != nil && res.Indices.Shards != nil {
		t = NewTable("Indices", "")
		t.Add("Count", res.Indices.Count)
		t.Add("Shards", fmt.Sprintf("%d (%d Primaries)", res.Indices.Shards.Total, res.Indices.Shards.Primaries))
		t.Add("Replication Ratio", res.Indices.Shards.Replication)
		t.Print()
	}

	if res.Indices != nil {
		t = NewTable("Storage", "")
		if res.Indices.Docs != nil {
			t.Add("Total Documents", fmt.Sprintf("%d (%d Deleted)", res.Indices.Docs.Count, res.Indices.Docs.Deleted))
			t.Add()
		}

		if res.Indices.Store != nil {
			t.Add("Store Size", res.Indices.Store.Size)
			t.Add("Store Throttle", res.Indices.Store.ThrottleTime)
			t.Add()
		}

		if res.Indices.FieldData != nil {
			t.Add("Field Data Size", res.Indices.FieldData.MemorySize)
			t.Add("Field Data Evictions", res.Indices.FieldData.Evictions)
			t.Add()
		}

		if res.Indices.FilterCache != nil {
			t.Add("Filter Cache Size", res.Indices.FilterCache.MemorySize)
			t.Add("Filter Cache Evictions", res.Indices.FilterCache.Evictions)
			t.Add()
		}

		if res.Indices.IdCache != nil {
			t.Add("ID Cache Size", res.Indices.IdCache.MemorySize)
		}

		if res.Indices.Completion != nil {
			t.Add("Completion Size", res.Indices.Completion.Size)
		}

		t.Print()
	}

	if res.Indices != nil && res.Indices.Percolate != nil {
		t = NewTable("Percolation", "")
		t.Add("Total", res.Indices.Percolate.Total)
		t.Add("Current", res.Indices.Percolate.Current)
		t.Add("Queries", res.Indices.Percolate.Queries)
		if res.Indices.Percolate.Time != "" {
			t.Add("Get Time", res.Indices.Percolate.Time)
		}
		if res.Indices.Percolate.MemorySizeInBytes > 0 {
			t.Add("Size", res.Indices.Percolate.MemorySize)
		}
		t.Print()
	}

	if res.Indices != nil && res.Indices.Segments != nil {
		t = NewTable("Segments", "")
		t.Add("Count", res.Indices.Segments.Count)
		t.Add("Size", res.Indices.Segments.Memory)
		t.Add()

		t.Add("Index Writer Size", fmt.Sprintf("%s (%s Max)", res.Indices.Segments.IndexWriterMemory, res.Indices.Segments.IndexWriterMaxMemory))
		t.Add("Version Map Size", res.Indices.Segments.VersionMapMemory)
		t.Add("Fixed Bit Set Size", res.Indices.Segments.FixedBitSet)
		t.Print()
	}

	if res.Nodes != nil {
		t = NewTable("Nodes", "")
		if res.Nodes.Count != nil {
			t.Add("Count", res.Nodes.Count.Total)
			t.Add("Client", res.Nodes.Count.Client)
			t.Add("Master", res.Nodes.Count.MasterOnly)
			t.Add("Data", res.Nodes.Count.DataOnly)
			t.Add("Master + Data", res.Nodes.Count.MasterData)
			t.Add()
		}

		t.Add("Processors", res.Nodes.OS.AvailableProcessors)
		t.Add("CPU Usage", fmt.Sprintf("%.2f%%", res.Nodes.Process.CPU.Percent))
		t.Add("File Descriptors", fmt.Sprintf("%d-%d (%d Avg)", res.Nodes.Process.OpenFileDescriptors.Min, res.Nodes.Process.OpenFileDescriptors.Max, res.Nodes.Process.OpenFileDescriptors.Avg))
		t.Add()

		t.Add("Total Memory", res.Nodes.OS.Mem.Total)
		t.Add("JVM Heap", fmt.Sprintf("%s (%s Max)", res.Nodes.JVM.Mem.HeapUsed, res.Nodes.JVM.Mem.HeapMax))
		t.Add("JVM Uptime", res.Nodes.JVM.MaxUptime)
		t.Add("JVM Threads", res.Nodes.JVM.Threads)
		t.Add()

		t.Add("Disk Total", res.Nodes.FS.Total)
		t.Add("Disk Free/Available", fmt.Sprintf("%s/%s", res.Nodes.FS.Free, res.Nodes.FS.Available))
		t.Add("Disk IO", fmt.Sprintf("%d (%d Read | %d Write)", res.Nodes.FS.DiskIOOp, res.Nodes.FS.DiskReads, res.Nodes.FS.DiskWrites))
		if res.Nodes.FS.DiskIOSize != "" {
			t.Add("Disk IO Size", fmt.Sprintf("%s (%s Read | %s Write)", res.Nodes.FS.DiskIOSize, res.Nodes.FS.DiskReadSize, res.Nodes.FS.DiskWriteSize))
		}
		t.Print()
	}

	if res.Nodes != nil && len(res.Nodes.Plugins) > 0 {
		t = NewTable("Plugins", "Version", "JVM", "Site", "URL", "Description")
		for _, plugin := range res.Nodes.Plugins {
			t.Add(plugin.Name, plugin.Version, plugin.JVM, plugin.Site, plugin.URL, plugin.Description)
		}
		t.Print()
	}
}

func (cn *EsConnection) getClusterNodes() {
	ctxb := context.Background()
	res, err := cn.Client.NodesInfo().Human(true).Do(ctxb)
	if err != nil {
		exitWithError(err)
	}

	t := NewTable("ID", "Process ID", "Name", "ES Version", "HTTP Address", "Transport Address")
	for id, node := range res.Nodes {
		t.Add(id, node.Process.ID, node.Name, node.Version, node.HTTPAddress, node.TransportAddress)
	}
	t.Print()
}

func (cn *EsConnection) putClusterSettings(settings string) {
	ctxb := context.Background()

	res, err := cn.Client.PerformRequest(ctxb, "PUT", "/_cluster/settings", url.Values{}, settings)
	if err != nil {
		exitWithError(err)
	}

	if res.StatusCode == http.StatusOK {
		fmt.Println("\nSettings updated succesfully.")
		return
	}

	var rerr elastic.Error
	err = json.Unmarshal(res.Body, &rerr)
	if err != nil {
		exitWithError(err)
	} else {
		exitWithError(&rerr)
	}
}
