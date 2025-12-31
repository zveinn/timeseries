# timeseries

A simple Go library for storing timeseries data on the filesystem. Data is CBOR encoded and organized by time into a directory structure like `year/month/day/hour.cbor`. Each hour file can hold multiple entries.

## Disclaimer
This is just a fun litle time series storage, not really meant for production use.

## Install

```
go get github.com/zveinn/timeseries
```

## Usage

```go
package main

import (
    "time"
    "github.com/zveinn/timeseries"
)

type Metric struct {
    Name  string
    Value float64
}

func main() {
    // init a client with a storage path
    client, err := timeseries.Init[Metric](timeseries.Options{
        Path: "./data",
    })
    if err != nil {
        panic(err)
    }

    // store some data
    client.Store(time.Now(), Metric{Name: "cpu", Value: 45.2})
    client.Store(time.Now(), Metric{Name: "memory", Value: 78.5})

    // get data for a time range (returns pointers)
    from := time.Now().Add(-time.Hour)
    to := time.Now()
    results, err := client.Get(from, to)

    // iterate over data with a callback (return true to continue, false to stop)
    var cpuMetrics []Metric
    client.Find(from, to, func(t time.Time, data Metric) bool {
        if data.Name == "cpu" && data.Value > 50.0 {
            cpuMetrics = append(cpuMetrics, data)
        }
        return true // continue iterating
    })

    // early stop: find first metric above threshold
    var found *Metric
    client.Find(from, to, func(t time.Time, data Metric) bool {
        if data.Value > 90.0 {
            found = &data
            return false // stop iteration
        }
        return true
    })

    // delete data in a time range
    client.Delete(from, to)
}
```

## API

- `Init[T any](opts Options) (*Client[T], error)` - create a new client, walks the directory to build a cache of existing files
- `Store(date time.Time, data T) error` - store data at a given time
- `Get(from, to time.Time) ([]*T, error)` - get all data in a time range
- `Find(from, to time.Time, fn func(time.Time, T) bool) error` - iterate over data in a time range; callback returns `true` to continue or `false` to stop early
- `Delete(from, to time.Time) error` - delete all hour files in a time range

## Benchmarks

Benchmarks run with 100,000 items (1 item per minute, ~69 days of data stored across ~1,667 hourly files).

```
goos: linux
goarch: amd64
pkg: github.com/zveinn/timeseries
cpu: AMD Ryzen 7 7840HS w/ Radeon 780M Graphics
BenchmarkA_Store-16            	       1	1022884720 ns/op	64124800 B/op	 1300513 allocs/op
BenchmarkB_Get-16              	      15	  79179142 ns/op	22050478 B/op	  420072 allocs/op
BenchmarkC1_Find_1Minute-16    	   29797	     40341 ns/op	    8637 B/op	     193 allocs/op
BenchmarkC2_Find_1Hour-16      	   28078	     41584 ns/op	   10527 B/op	     252 allocs/op
BenchmarkC3_Find_1Day-16       	    1152	   1025923 ns/op	  252657 B/op	    6048 allocs/op
BenchmarkC4_Find_1Week-16      	     168	   7089725 ns/op	 1768585 B/op	   42337 allocs/op
BenchmarkC5_Find_1Month-16     	      38	  30561462 ns/op	 7579674 B/op	  181446 allocs/op
BenchmarkC6_Find_Full-16       	      15	  70822510 ns/op	17546298 B/op	  420019 allocs/op
BenchmarkD_GetSubset-16        	     159	   7448360 ns/op	 2068287 B/op	   42084 allocs/op
BenchmarkE_CacheLookup-16      	     376	   3143373 ns/op	       0 B/op	       0 allocs/op
BenchmarkF_CacheInit-16        	     223	   5185070 ns/op	  925247 B/op	   17713 allocs/op
```

Run benchmarks with:
```
go test -bench=. -benchmem
```
