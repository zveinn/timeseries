package timeseries

import (
	"fmt"
	"runtime"
	"time"
)

type Options struct {
	Debug       bool
	PrintMemory bool
	Path        string
}

type Client[T any] struct {
	Cache [366][12][31][24]T
	Opts  Options
}

// Init creates a new timeseries client.
// NOTE: The client contains a cache for timeseries data
// within it's 'Path', would not recommend initializing
// two client for the same path unless you are made out of RAM
func Init[T any](opts Options) (client *Client[T], err error) {
	client = new(Client[T])
	client.Opts = opts

	if opts.Debug {
		go initDebugPrintLoop(opts)
	}

	return
}

func initDebugPrintLoop(opts Options) {
	for {
		if opts.PrintMemory {
			printMemUsage()
		}
		time.Sleep(1 * time.Second)
	}
}

func printMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Alloc = %v MiB", m.Alloc/1024/1024)
	fmt.Printf("\tTotalAlloc = %v MiB", m.TotalAlloc/1024/1024)
	fmt.Printf("\tSys = %v MiB", m.Sys/1024/1024)
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}
