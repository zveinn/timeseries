package timeseries

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/fxamacker/cbor/v2"
)

type Options struct {
	Debug       bool
	PrintMemory bool
	Path        string
}

type Entry[T any] struct {
	Time time.Time
	Data T
}

type Client[T any] struct {
	Cache map[int]*[12][31][24][60]struct{}
	Opts  Options
}

func Init[T any](opts Options) (client *Client[T], err error) {
	client = new(Client[T])
	client.Opts = opts
	client.Cache = make(map[int]*[12][31][24][60]struct{})

	if opts.Path != "" {
		err = client.buildCache()
		if err != nil {
			return nil, err
		}
	}

	if opts.Debug {
		go initDebugPrintLoop(opts)
	}

	return
}

func (c *Client[T]) buildCache() error {
	if _, err := os.Stat(c.Opts.Path); os.IsNotExist(err) {
		return nil
	}

	return filepath.Walk(c.Opts.Path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if filepath.Ext(path) != ".cbor" {
			return nil
		}

		t, parseErr := c.parsePathToTime(path)
		if parseErr != nil {
			return nil
		}

		c.setCache(t)
		return nil
	})
}

func (c *Client[T]) setCache(t time.Time) {
	y, m, d, h, min := t.Year(), int(t.Month())-1, t.Day()-1, t.Hour(), t.Minute()
	if c.Cache[y] == nil {
		c.Cache[y] = new([12][31][24][60]struct{})
	}
	c.Cache[y][m][d][h][min] = struct{}{}
}

func (c *Client[T]) getCache(t time.Time) bool {
	y, m, d, h, min := t.Year(), int(t.Month())-1, t.Day()-1, t.Hour(), t.Minute()
	if c.Cache[y] == nil {
		return false
	}
	return c.Cache[y][m][d][h][min] == struct{}{}
}

func (c *Client[T]) parsePathToTime(path string) (time.Time, error) {
	rel, err := filepath.Rel(c.Opts.Path, path)
	if err != nil {
		return time.Time{}, err
	}

	dir := filepath.Dir(rel)
	parts := filepath.SplitList(dir)
	if len(parts) == 1 {
		parts = []string{}
		current := dir
		for current != "." && current != "" {
			parts = append([]string{filepath.Base(current)}, parts...)
			current = filepath.Dir(current)
		}
	}

	if len(parts) != 4 {
		return time.Time{}, errors.New("invalid path structure")
	}

	year, err := strconv.Atoi(parts[0])
	if err != nil {
		return time.Time{}, err
	}

	month, err := strconv.Atoi(parts[1])
	if err != nil {
		return time.Time{}, err
	}

	day, err := strconv.Atoi(parts[2])
	if err != nil {
		return time.Time{}, err
	}

	hour, err := strconv.Atoi(parts[3])
	if err != nil {
		return time.Time{}, err
	}

	filename := filepath.Base(path)
	minuteStr := filename[:len(filename)-5]
	minute, err := strconv.Atoi(minuteStr)
	if err != nil {
		return time.Time{}, err
	}

	return time.Date(year, time.Month(month), day, hour, minute, 0, 0, time.UTC), nil
}

func (c *Client[T]) timeToPath(t time.Time) string {
	return filepath.Join(
		c.Opts.Path,
		fmt.Sprintf("%04d", t.Year()),
		fmt.Sprintf("%02d", int(t.Month())),
		fmt.Sprintf("%02d", t.Day()),
		fmt.Sprintf("%02d", t.Hour()),
		fmt.Sprintf("%02d.cbor", t.Minute()),
	)
}

func (c *Client[T]) Store(date time.Time, data T) error {
	truncated := date.Truncate(time.Minute)
	path := c.timeToPath(truncated)

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	entry := Entry[T]{
		Time: date,
		Data: data,
	}

	encoded, err := cbor.Marshal(entry)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	n, err := f.Write(encoded)
	if err != nil {
		return err
	}

	if n != len(encoded) {
		return errors.New("write verification failed: bytes written != encoded length")
	}

	c.setCache(truncated)

	return nil
}

func (c *Client[T]) Get(from time.Time, to time.Time) ([]*T, error) {
	var results []*T

	err := c.Find(from, to, func(t time.Time, data T) {
		dataCopy := data
		results = append(results, &dataCopy)
	})

	return results, err
}

func (c *Client[T]) Find(from time.Time, to time.Time, fn func(t time.Time, data T)) error {
	fromTrunc := from.Truncate(time.Minute)
	toTrunc := to.Truncate(time.Minute).Add(time.Minute)

	for current := fromTrunc; current.Before(toTrunc); current = current.Add(time.Minute) {
		path := c.timeToPath(current)

		if !c.getCache(current) {
			// Cache miss - check if file exists on disk
			if _, err := os.Stat(path); os.IsNotExist(err) {
				continue
			}
			// File exists but wasn't cached, update cache
			c.setCache(current)
		}

		if err := c.readFile(path, from, to, fn); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client[T]) readFile(path string, from time.Time, to time.Time, fn func(t time.Time, data T)) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	dec := cbor.NewDecoder(f)

	for {
		var entry Entry[T]
		err := dec.Decode(&entry)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if (entry.Time.Equal(from) || entry.Time.After(from)) &&
			(entry.Time.Equal(to) || entry.Time.Before(to)) {
			fn(entry.Time, entry.Data)
		}
	}

	return nil
}

func (c *Client[T]) Delete(from time.Time, to time.Time) error {
	fromTrunc := from.Truncate(time.Minute)
	toTrunc := to.Truncate(time.Minute)

	for current := fromTrunc; current.Before(toTrunc); current = current.Add(time.Minute) {
		if !c.getCache(current) {
			continue
		}

		path := c.timeToPath(current)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		c.setCache(current)
		c.cleanEmptyDirs(filepath.Dir(path))
	}

	return nil
}

func (c *Client[T]) cleanEmptyDirs(dir string) {
	for dir != c.Opts.Path && dir != "" {
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) > 0 {
			break
		}
		os.Remove(dir)
		dir = filepath.Dir(dir)
	}
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
