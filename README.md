# timeseries

A simple Go library for storing timeseries data on the filesystem. Data is CBOR encoded and organized by time into a directory structure like `year/month/day/hour/minute.cbor`. Each minute file can hold multiple entries.

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

    // iterate over data with a callback
    var cpuMetrics []Metric
    client.Find(from, to, func(t time.Time, data Metric) {
        if data.Name == "cpu" && data.Value > 50.0 {
            cpuMetrics = append(cpuMetrics, data)
        }
    })

    // delete data in a time range
    client.Delete(from, to)
}
```

## API

- `Init[T any](opts Options) (*Client[T], error)` - create a new client, walks the directory to build a cache of existing files
- `Store(date time.Time, data T) error` - store data at a given time
- `Get(from, to time.Time) ([]*T, error)` - get all data in a time range
- `Find(from, to time.Time, fn func(time.Time, T)) error` - iterate over data in a time range
- `Delete(from, to time.Time) error` - delete all minute files in a time range

## Benchmarks

Benchmarks run with 100,000 items (1 item per minute, ~69 days of data). Each Find benchmark searches for an item at the end of its time window.

```
goos: linux
goarch: amd64
cpu: AMD Ryzen 7 7840HS w/ Radeon 780M Graphics
```

### Core Operations

| Operation | Time | Allocs | Description |
|-----------|------|--------|-------------|
| Store | 1.43s | 1.4M | Store 100,000 items |
| Get | 970ms | 1.6M | Retrieve all 100,000 items |
| Delete | 1.80s | 7.7M | Delete all 100,000 items |

### Find by Time Window

| Window | Items | Time | Allocs |
|--------|-------|------|--------|
| 1 minute | 1 | 89µs | 20 |
| 1 hour | 60 | 699µs | 964 |
| 1 day | 1,440 | 13ms | 23k |
| 1 week | 10,080 | 93ms | 161k |
| 1 month | 43,200 | 400ms | 691k |
| Full (69 days) | 100,000 | 931ms | 1.6M |

### Other

| Operation | Time | Allocs | Description |
|-----------|------|--------|-------------|
| GetSubset | 97ms | 160k | Retrieve 10% of data (10,000 items) |
| CacheLookup | 4ms | 0 | 100,000 in-memory cache lookups |
| CacheInit | 316ms | 1.2M | Rebuild cache from 100,000 files |

Run benchmarks with:
```
go test -bench=. -benchmem
```
