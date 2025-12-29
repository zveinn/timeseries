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
