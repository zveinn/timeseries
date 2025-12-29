package timeseries

import (
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
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

type CacheEntry struct {
	Time time.Time
	Path string
}

type Client[T any] struct {
	Cache []CacheEntry
	Opts  Options
}

func Init[T any](opts Options) (client *Client[T], err error) {
	client = new(Client[T])
	client.Opts = opts

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
	c.Cache = make([]CacheEntry, 0)

	if _, err := os.Stat(c.Opts.Path); os.IsNotExist(err) {
		return nil
	}

	err := filepath.Walk(c.Opts.Path, func(path string, info os.FileInfo, err error) error {
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

		c.Cache = append(c.Cache, CacheEntry{
			Time: t,
			Path: path,
		})

		return nil
	})

	if err != nil {
		return err
	}

	sort.Slice(c.Cache, func(i, j int) bool {
		return c.Cache[i].Time.Before(c.Cache[j].Time)
	})

	return nil
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
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	entry := Entry[T]{
		Time: date,
		Data: data,
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	startPos, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	hasher := md5.New()
	writer := io.MultiWriter(f, hasher)

	enc := cbor.NewEncoder(writer)
	if err := enc.Encode(entry); err != nil {
		return err
	}

	expectedHash := hasher.Sum(nil)

	if err := f.Sync(); err != nil {
		return err
	}

	verifyFile, err := os.Open(path)
	if err != nil {
		return err
	}
	defer verifyFile.Close()

	if _, err := verifyFile.Seek(startPos, io.SeekStart); err != nil {
		return err
	}

	verifyHasher := md5.New()
	if _, err := io.Copy(verifyHasher, verifyFile); err != nil {
		return err
	}

	actualHash := verifyHasher.Sum(nil)
	for i := range expectedHash {
		if expectedHash[i] != actualHash[i] {
			return errors.New("md5 verification failed after write")
		}
	}

	c.updateCache(truncated, path)

	return nil
}

func (c *Client[T]) updateCache(t time.Time, path string) {
	for _, entry := range c.Cache {
		if entry.Time.Equal(t) {
			return
		}
	}

	c.Cache = append(c.Cache, CacheEntry{
		Time: t,
		Path: path,
	})

	sort.Slice(c.Cache, func(i, j int) bool {
		return c.Cache[i].Time.Before(c.Cache[j].Time)
	})
}

func (c *Client[T]) removeFromCache(t time.Time) {
	for i, entry := range c.Cache {
		if entry.Time.Equal(t) {
			c.Cache = append(c.Cache[:i], c.Cache[i+1:]...)
			return
		}
	}
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

	for _, entry := range c.Cache {
		if entry.Time.Before(fromTrunc) {
			continue
		}
		if entry.Time.After(toTrunc) || entry.Time.Equal(toTrunc) {
			break
		}

		if err := c.readFile(entry.Path, from, to, fn); err != nil {
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

	var toRemove []CacheEntry

	for _, entry := range c.Cache {
		if entry.Time.Before(fromTrunc) {
			continue
		}
		if entry.Time.After(toTrunc) || entry.Time.Equal(toTrunc) {
			break
		}
		toRemove = append(toRemove, entry)
	}

	for _, entry := range toRemove {
		if err := os.Remove(entry.Path); err != nil && !os.IsNotExist(err) {
			return err
		}
		c.removeFromCache(entry.Time)

		dir := filepath.Dir(entry.Path)
		c.cleanEmptyDirs(dir)
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
