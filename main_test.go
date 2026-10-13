package timeseries

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
)

type testStruct struct {
	SomeString string
	SomeInt    int
	SomeFloat  float64
}

type nestedStruct struct {
	Inner    testStruct
	InnerPtr *testStruct
	Name     string
}

type structWithSlice struct {
	Items  []string
	Values []int
}

type structWithMap struct {
	Data map[string]int
}

type largeStruct struct {
	Field1  string
	Field2  string
	Field3  string
	Field4  int
	Field5  int
	Field6  float64
	Field7  float64
	Field8  []byte
	Field9  []int
	Field10 map[string]string
}

func Test_init(t *testing.T) {
	c, err := Init[testStruct](Options{
		Debug:       false,
		PrintMemory: false,
	})
	if c == nil || err != nil {
		t.Fatal("unable to init")
	}
}

func Test_StoreAndGet(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{
		Path: tmpDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Date(2024, 6, 15, 10, 30, 45, 0, time.UTC)
	data := testStruct{
		SomeString: "test",
		SomeInt:    42,
		SomeFloat:  3.14,
	}

	err = c.Store(now, data)
	if err != nil {
		t.Fatal(err)
	}

	expectedPath := filepath.Join(tmpDir, "2024", "06", "15", "10.cbor")
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Fatalf("expected file at %s", expectedPath)
	}

	results, err := c.Get(now.Add(-time.Minute), now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0].SomeString != "test" || results[0].SomeInt != 42 || results[0].SomeFloat != 3.14 {
		t.Fatal("data mismatch")
	}
}

func Test_StoreMultipleInSameMinute(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{
		Path: tmpDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	baseTime := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)

	for i := 0; i < 5; i++ {
		data := testStruct{
			SomeString: "test",
			SomeInt:    i,
			SomeFloat:  float64(i),
		}
		err = c.Store(baseTime.Add(time.Duration(i)*time.Second), data)
		if err != nil {
			t.Fatal(err)
		}
	}

	results, err := c.Get(baseTime.Add(-time.Minute), baseTime.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
}

func Test_Find(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{
		Path: tmpDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	baseTime := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)

	for i := 0; i < 3; i++ {
		data := testStruct{
			SomeString: "test",
			SomeInt:    i,
			SomeFloat:  float64(i),
		}
		err = c.Store(baseTime.Add(time.Duration(i)*time.Minute), data)
		if err != nil {
			t.Fatal(err)
		}
	}

	count := 0
	err = c.Find(baseTime.Add(-time.Minute), baseTime.Add(5*time.Minute), func(tm time.Time, data testStruct) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	if count != 3 {
		t.Fatalf("expected 3 items, got %d", count)
	}
}

func Test_Delete(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{
		Path: tmpDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	baseTime := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)

	// Store items in 3 different hours
	for i := 0; i < 3; i++ {
		data := testStruct{
			SomeString: "test",
			SomeInt:    i,
			SomeFloat:  float64(i),
		}
		err = c.Store(baseTime.Add(time.Duration(i)*time.Hour), data)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Delete the first 2 hours (10:00 and 11:00)
	err = c.Delete(baseTime, baseTime.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	results, err := c.Get(baseTime.Add(-time.Hour), baseTime.Add(5*time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result after delete, got %d", len(results))
	}

	if results[0].SomeInt != 2 {
		t.Fatal("wrong item remaining")
	}
}

func Test_CachePopulationOnInit(t *testing.T) {
	tmpDir := t.TempDir()

	c1, err := Init[testStruct](Options{
		Path: tmpDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	baseTime := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)

	for i := 0; i < 3; i++ {
		data := testStruct{
			SomeString: "test",
			SomeInt:    i,
			SomeFloat:  float64(i),
		}
		err = c1.Store(baseTime.Add(time.Duration(i)*time.Minute), data)
		if err != nil {
			t.Fatal(err)
		}
	}

	c2, err := Init[testStruct](Options{
		Path: tmpDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	results, err := c2.Get(baseTime.Add(-time.Minute), baseTime.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
}

func Test_NestedStruct(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[nestedStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	inner := testStruct{SomeString: "inner", SomeInt: 100, SomeFloat: 1.5}
	data := nestedStruct{
		Inner:    inner,
		InnerPtr: &inner,
		Name:     "nested",
	}

	err = c.Store(now, data)
	if err != nil {
		t.Fatal(err)
	}

	results, err := c.Get(now, now)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0].Inner.SomeString != "inner" || results[0].Name != "nested" {
		t.Fatal("nested struct data mismatch")
	}
}

func Test_StructWithSlice(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[structWithSlice](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	data := structWithSlice{
		Items:  []string{"a", "b", "c"},
		Values: []int{1, 2, 3, 4, 5},
	}

	err = c.Store(now, data)
	if err != nil {
		t.Fatal(err)
	}

	results, err := c.Get(now, now)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if len(results[0].Items) != 3 || len(results[0].Values) != 5 {
		t.Fatal("slice data mismatch")
	}
}

func Test_StructWithMap(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[structWithMap](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	data := structWithMap{
		Data: map[string]int{"one": 1, "two": 2, "three": 3},
	}

	err = c.Store(now, data)
	if err != nil {
		t.Fatal(err)
	}

	results, err := c.Get(now, now)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0].Data["one"] != 1 || results[0].Data["two"] != 2 {
		t.Fatal("map data mismatch")
	}
}

func Test_LargeStruct(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[largeStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	data := largeStruct{
		Field1:  "string1",
		Field2:  "string2",
		Field3:  "string3",
		Field4:  100,
		Field5:  200,
		Field6:  1.1,
		Field7:  2.2,
		Field8:  []byte{0x01, 0x02, 0x03},
		Field9:  []int{1, 2, 3},
		Field10: map[string]string{"key": "value"},
	}

	err = c.Store(now, data)
	if err != nil {
		t.Fatal(err)
	}

	results, err := c.Get(now, now)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0].Field1 != "string1" || results[0].Field4 != 100 {
		t.Fatal("large struct data mismatch")
	}
}

func Test_YearBoundary(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	dec31 := time.Date(2024, 12, 31, 23, 59, 0, 0, time.UTC)
	jan1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	err = c.Store(dec31, testStruct{SomeString: "dec31", SomeInt: 1, SomeFloat: 1.0})
	if err != nil {
		t.Fatal(err)
	}

	err = c.Store(jan1, testStruct{SomeString: "jan1", SomeInt: 2, SomeFloat: 2.0})
	if err != nil {
		t.Fatal(err)
	}

	results, err := c.Get(dec31, jan1)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results across year boundary, got %d", len(results))
	}
}

func Test_MidnightBoundary(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	before := time.Date(2024, 6, 15, 23, 59, 0, 0, time.UTC)
	after := time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC)

	err = c.Store(before, testStruct{SomeString: "before", SomeInt: 1, SomeFloat: 1.0})
	if err != nil {
		t.Fatal(err)
	}

	err = c.Store(after, testStruct{SomeString: "after", SomeInt: 2, SomeFloat: 2.0})
	if err != nil {
		t.Fatal(err)
	}

	results, err := c.Get(before, after)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results across midnight, got %d", len(results))
	}
}

func Test_MonthBoundary(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	jan31 := time.Date(2024, 1, 31, 23, 59, 0, 0, time.UTC)
	feb1 := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)

	err = c.Store(jan31, testStruct{SomeString: "jan31", SomeInt: 1, SomeFloat: 1.0})
	if err != nil {
		t.Fatal(err)
	}

	err = c.Store(feb1, testStruct{SomeString: "feb1", SomeInt: 2, SomeFloat: 2.0})
	if err != nil {
		t.Fatal(err)
	}

	results, err := c.Get(jan31, feb1)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results across month boundary, got %d", len(results))
	}
}

func Test_LeapYear(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	feb29 := time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC)

	err = c.Store(feb29, testStruct{SomeString: "leap", SomeInt: 29, SomeFloat: 2.9})
	if err != nil {
		t.Fatal(err)
	}

	results, err := c.Get(feb29, feb29)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result on leap day, got %d", len(results))
	}

	if results[0].SomeString != "leap" {
		t.Fatal("leap year data mismatch")
	}
}

func Test_EmptyTimeRange(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	err = c.Store(now, testStruct{SomeString: "test", SomeInt: 1, SomeFloat: 1.0})
	if err != nil {
		t.Fatal(err)
	}

	earlier := time.Date(2024, 6, 15, 9, 0, 0, 0, time.UTC)
	results, err := c.Get(earlier, earlier.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 0 {
		t.Fatalf("expected 0 results for empty range, got %d", len(results))
	}
}

func Test_QueryBeforeAllData(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	dataTime := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	err = c.Store(dataTime, testStruct{SomeString: "test", SomeInt: 1, SomeFloat: 1.0})
	if err != nil {
		t.Fatal(err)
	}

	queryStart := time.Date(2024, 6, 15, 8, 0, 0, 0, time.UTC)
	queryEnd := time.Date(2024, 6, 15, 9, 0, 0, 0, time.UTC)

	results, err := c.Get(queryStart, queryEnd)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 0 {
		t.Fatalf("expected 0 results before data, got %d", len(results))
	}
}

func Test_QueryAfterAllData(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	dataTime := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	err = c.Store(dataTime, testStruct{SomeString: "test", SomeInt: 1, SomeFloat: 1.0})
	if err != nil {
		t.Fatal(err)
	}

	queryStart := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	queryEnd := time.Date(2024, 6, 15, 13, 0, 0, 0, time.UTC)

	results, err := c.Get(queryStart, queryEnd)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 0 {
		t.Fatalf("expected 0 results after data, got %d", len(results))
	}
}

func Test_MinuteEdges(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	min0 := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	min59 := time.Date(2024, 6, 15, 10, 59, 0, 0, time.UTC)

	err = c.Store(min0, testStruct{SomeString: "min0", SomeInt: 0, SomeFloat: 0.0})
	if err != nil {
		t.Fatal(err)
	}

	err = c.Store(min59, testStruct{SomeString: "min59", SomeInt: 59, SomeFloat: 59.0})
	if err != nil {
		t.Fatal(err)
	}

	results, err := c.Get(min0, min59)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results at minute edges, got %d", len(results))
	}
}

func Test_MultipleYears(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	years := []int{2020, 2021, 2022, 2023, 2024}
	for _, year := range years {
		tm := time.Date(year, 6, 15, 10, 30, 0, 0, time.UTC)
		err = c.Store(tm, testStruct{SomeString: "year", SomeInt: year, SomeFloat: float64(year)})
		if err != nil {
			t.Fatal(err)
		}
	}

	from := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 12, 31, 23, 59, 0, 0, time.UTC)

	results, err := c.Get(from, to)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 5 {
		t.Fatalf("expected 5 results across years, got %d", len(results))
	}
}

func Test_ExactTimeMatch(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	storeTime := time.Date(2024, 6, 15, 10, 30, 45, 0, time.UTC)
	err = c.Store(storeTime, testStruct{SomeString: "exact", SomeInt: 1, SomeFloat: 1.0})
	if err != nil {
		t.Fatal(err)
	}

	queryStart := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	queryEnd := time.Date(2024, 6, 15, 10, 30, 59, 0, time.UTC)
	results, err := c.Get(queryStart, queryEnd)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result for time range, got %d", len(results))
	}
}

func Test_SameFromToTime(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	storeTime := time.Date(2024, 6, 15, 10, 30, 30, 0, time.UTC)
	err = c.Store(storeTime, testStruct{SomeString: "same", SomeInt: 1, SomeFloat: 1.0})
	if err != nil {
		t.Fatal(err)
	}

	results, err := c.Get(storeTime, storeTime)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result for same from/to, got %d", len(results))
	}
}

func Test_NonExistentPathInit(t *testing.T) {
	c, err := Init[testStruct](Options{Path: "/nonexistent/path/that/doesnt/exist"})
	if err != nil {
		t.Fatal(err)
	}

	if c == nil {
		t.Fatal("client should not be nil")
	}
}

func Test_EmptyPath(t *testing.T) {
	c, err := Init[testStruct](Options{Path: ""})
	if err != nil {
		t.Fatal(err)
	}

	if c == nil {
		t.Fatal("client should not be nil")
	}
}

func Test_DeleteNonExistent(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	from := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 15, 11, 0, 0, 0, time.UTC)

	err = c.Delete(from, to)
	if err != nil {
		t.Fatal("delete of non-existent data should not error")
	}
}

func Test_FindCallback(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	baseTime := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		err = c.Store(baseTime.Add(time.Duration(i)*time.Minute), testStruct{
			SomeString: "test",
			SomeInt:    i,
			SomeFloat:  float64(i),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	var times []time.Time
	var values []int

	err = c.Find(baseTime, baseTime.Add(4*time.Minute), func(tm time.Time, data testStruct) bool {
		times = append(times, tm)
		values = append(values, data.SomeInt)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(times) != 5 {
		t.Fatalf("expected 5 callbacks, got %d", len(times))
	}

	for i, v := range values {
		if v != i {
			t.Fatalf("expected value %d at index %d, got %d", i, i, v)
		}
	}
}

func Test_FindEarlyStop(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	baseTime := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		err = c.Store(baseTime.Add(time.Duration(i)*time.Minute), testStruct{
			SomeString: "test",
			SomeInt:    i,
			SomeFloat:  float64(i),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Stop after finding 3 items
	count := 0
	err = c.Find(baseTime, baseTime.Add(9*time.Minute), func(tm time.Time, data testStruct) bool {
		count++
		return count < 3 // stop after 3rd item
	})
	if err != nil {
		t.Fatal(err)
	}

	if count != 3 {
		t.Fatalf("expected 3 callbacks before stop, got %d", count)
	}

	// Stop immediately on first item
	count = 0
	err = c.Find(baseTime, baseTime.Add(9*time.Minute), func(tm time.Time, data testStruct) bool {
		count++
		return false // stop immediately
	})
	if err != nil {
		t.Fatal(err)
	}

	if count != 1 {
		t.Fatalf("expected 1 callback before stop, got %d", count)
	}
}

func Test_FindEarlyStopAcrossHours(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	// Store items across 3 different hours
	baseTime := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		err = c.Store(baseTime.Add(time.Duration(i)*time.Hour), testStruct{
			SomeString: "test",
			SomeInt:    i,
			SomeFloat:  float64(i),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Stop after finding item in second hour
	count := 0
	err = c.Find(baseTime, baseTime.Add(3*time.Hour), func(tm time.Time, data testStruct) bool {
		count++
		return data.SomeInt < 1 // stop after finding SomeInt == 1
	})
	if err != nil {
		t.Fatal(err)
	}

	if count != 2 {
		t.Fatalf("expected 2 callbacks (stopping after 2nd hour), got %d", count)
	}
}

func Test_PrimitiveTypes(t *testing.T) {
	tmpDir := t.TempDir()

	cInt, _ := Init[int](Options{Path: filepath.Join(tmpDir, "int")})
	cString, _ := Init[string](Options{Path: filepath.Join(tmpDir, "string")})
	cFloat, _ := Init[float64](Options{Path: filepath.Join(tmpDir, "float")})

	now := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)

	cInt.Store(now, 42)
	cString.Store(now, "hello")
	cFloat.Store(now, 3.14159)

	intResults, _ := cInt.Get(now, now)
	stringResults, _ := cString.Get(now, now)
	floatResults, _ := cFloat.Get(now, now)

	if len(intResults) != 1 || *intResults[0] != 42 {
		t.Fatal("int type failed")
	}

	if len(stringResults) != 1 || *stringResults[0] != "hello" {
		t.Fatal("string type failed")
	}

	if len(floatResults) != 1 || *floatResults[0] != 3.14159 {
		t.Fatal("float type failed")
	}
}

func Test_LargeDataVolume(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	baseTime := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	count := 100

	for i := 0; i < count; i++ {
		err = c.Store(baseTime.Add(time.Duration(i)*time.Minute), testStruct{
			SomeString: "bulk",
			SomeInt:    i,
			SomeFloat:  float64(i),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	results, err := c.Get(baseTime, baseTime.Add(time.Duration(count-1)*time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != count {
		t.Fatalf("expected %d results, got %d", count, len(results))
	}
}

func Test_ConcurrentMinuteWrites(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	baseTime := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)

	for i := 0; i < 100; i++ {
		err = c.Store(baseTime.Add(time.Duration(i)*time.Millisecond), testStruct{
			SomeString: "concurrent",
			SomeInt:    i,
			SomeFloat:  float64(i),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	results, err := c.Get(baseTime, baseTime)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 100 {
		t.Fatalf("expected 100 results in same minute, got %d", len(results))
	}
}

func Test_FindUncachedDataOnDisk(t *testing.T) {
	dir := t.TempDir()

	// First, create a client and store some data
	c1, err := Init[testStruct](Options{Path: dir})
	if err != nil {
		t.Fatal(err)
	}

	baseTime := time.Date(2024, 6, 15, 10, 30, 15, 0, time.UTC)
	err = c1.Store(baseTime, testStruct{
		SomeString: "test",
		SomeInt:    42,
		SomeFloat:  3.14,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create a new client WITHOUT building the cache (empty cache)
	c2 := &Client[testStruct]{
		Cache: make(map[int]*[12][31][24]struct{}),
		Opts:  Options{Path: dir},
	}

	// Verify cache is empty
	if c2.getCache(baseTime.Truncate(time.Hour)) {
		t.Fatal("expected cache to be empty")
	}

	// Find should still locate the data on disk
	results, err := c2.Get(baseTime, baseTime)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result from disk lookup, got %d", len(results))
	}

	if results[0].SomeInt != 42 {
		t.Fatalf("expected SomeInt=42, got %d", results[0].SomeInt)
	}

	// Cache should now be populated after the Find
	if !c2.getCache(baseTime.Truncate(time.Hour)) {
		t.Fatal("expected cache to be populated after Find")
	}
}

func Test_CompressedRecords(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	base := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	payload := strings.Repeat("timeseries-payload-", 80)
	const count = 20

	for i := 0; i < count; i++ {
		err = c.Store(base.Add(time.Duration(i)*time.Second), testStruct{
			SomeString: payload,
			SomeInt:    i,
			SomeFloat:  float64(i),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	path := filepath.Join(tmpDir, "2024", "06", "15", "10.cbor")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(raw) < 2 || raw[0] != 0x1f || raw[1] != 0x8b {
		t.Fatal("expected gzip compressed records")
	}

	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()

	dec := cbor.NewDecoder(zr)
	var decoded int
	for {
		var entry Entry[testStruct]
		if err := dec.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		if entry.Data.SomeString != payload || entry.Data.SomeInt != decoded {
			t.Fatalf("record %d mismatch", decoded)
		}
		decoded++
	}
	if decoded != count {
		t.Fatalf("expected %d compressed records, got %d", count, decoded)
	}

	results, err := c.Get(base, base.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != count {
		t.Fatalf("expected %d results, got %d", count, len(results))
	}
	if results[count-1].SomeInt != count-1 || results[count-1].SomeString != payload {
		t.Fatal("round trip mismatch")
	}
}

func gzipMembers(t *testing.T, raw []byte) [][]byte {
	t.Helper()

	r := bytes.NewReader(raw)
	var members [][]byte
	for r.Len() > 0 {
		zr, err := gzip.NewReader(r)
		if err != nil {
			t.Fatal(err)
		}
		zr.Multistream(false)

		body, err := io.ReadAll(zr)
		if err != nil {
			t.Fatal(err)
		}
		if err := zr.Close(); err != nil {
			t.Fatal(err)
		}
		members = append(members, body)
	}
	return members
}

func Test_StoreAppendsIndependentGzipMembers(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	base := time.Date(2024, 6, 15, 10, 15, 0, 0, time.UTC)
	path := filepath.Join(tmpDir, "2024", "06", "15", "10.cbor")

	first := testStruct{SomeString: "first", SomeInt: 1, SomeFloat: 1.5}
	if err := c.Store(base, first); err != nil {
		t.Fatal(err)
	}

	afterFirst, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(gzipMembers(t, afterFirst)) != 1 {
		t.Fatal("expected one gzip member after the first store")
	}

	secondAt := base.Add(20 * time.Second)
	second := testStruct{SomeString: "second", SomeInt: 2, SomeFloat: 2.5}
	if err := c.Store(secondAt, second); err != nil {
		t.Fatal(err)
	}

	afterSecond, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(afterSecond, afterFirst) {
		t.Fatal("second store rewrote the existing compressed record")
	}
	if len(afterSecond) <= len(afterFirst) {
		t.Fatal("second store did not append a compressed record")
	}

	members := gzipMembers(t, afterSecond)
	if len(members) != 2 {
		t.Fatalf("expected 2 gzip members, got %d", len(members))
	}

	want := []Entry[testStruct]{
		{Time: base, Data: first},
		{Time: secondAt, Data: second},
	}
	for i, member := range members {
		var got Entry[testStruct]
		if err := cbor.Unmarshal(member, &got); err != nil {
			t.Fatal(err)
		}
		if !got.Time.Equal(want[i].Time) || got.Data != want[i].Data {
			t.Fatalf("member %d mismatch: got %+v", i, got)
		}

		var raw bytes.Buffer
		if err := cbor.NewEncoder(&raw).Encode(want[i]); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(member, raw.Bytes()) {
			t.Fatalf("member %d is not the plain cbor encoding", i)
		}
	}
}

func Test_CompressionShrinksRepetitivePayload(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	base := time.Date(2024, 3, 2, 4, 5, 6, 0, time.UTC)
	payload := strings.Repeat("aaaa-bbbb-cccc-dddd-", 100)
	const count = 30

	var raw bytes.Buffer
	enc := cbor.NewEncoder(&raw)
	for i := 0; i < count; i++ {
		entry := Entry[testStruct]{
			Time: base.Add(time.Duration(i) * time.Second),
			Data: testStruct{SomeString: payload, SomeInt: i, SomeFloat: float64(i)},
		}
		if err := enc.Encode(entry); err != nil {
			t.Fatal(err)
		}
		if err := c.Store(entry.Time, entry.Data); err != nil {
			t.Fatal(err)
		}
	}

	path := filepath.Join(tmpDir, "2024", "03", "02", "04.cbor")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() >= int64(raw.Len()) {
		t.Fatalf("compressed file is %d bytes, raw cbor is %d bytes", info.Size(), raw.Len())
	}

	results, err := c.Get(base, base.Add(time.Duration(count-1)*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != count {
		t.Fatalf("expected %d results, got %d", count, len(results))
	}
	if results[0].SomeString != payload || results[count-1].SomeInt != count-1 {
		t.Fatal("compressed payload mismatch")
	}
}

func Test_ReinitReadsCompressedFiles(t *testing.T) {
	tmpDir := t.TempDir()

	c1, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	storedAt := time.Date(2024, 11, 8, 21, 4, 30, 0, time.UTC)
	data := testStruct{SomeString: "reload", SomeInt: 7, SomeFloat: 8.5}
	if err := c1.Store(storedAt, data); err != nil {
		t.Fatal(err)
	}

	c2, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	var got []testStruct
	var gotTimes []time.Time
	err = c2.Find(storedAt, storedAt, func(tm time.Time, item testStruct) bool {
		gotTimes = append(gotTimes, tm)
		got = append(got, item)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != data || !gotTimes[0].Equal(storedAt) {
		t.Fatalf("reloaded compressed record mismatch: %+v at %v", got, gotTimes)
	}
}

func Test_CompressedRecordsAcrossHours(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	firstAt := time.Date(2024, 6, 15, 10, 59, 0, 0, time.UTC)
	secondAt := time.Date(2024, 6, 15, 11, 0, 0, 0, time.UTC)
	if err := c.Store(firstAt, testStruct{SomeString: "h10", SomeInt: 10, SomeFloat: 10}); err != nil {
		t.Fatal(err)
	}
	if err := c.Store(secondAt, testStruct{SomeString: "h11", SomeInt: 11, SomeFloat: 11}); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"10.cbor", "11.cbor"} {
		raw, err := os.ReadFile(filepath.Join(tmpDir, "2024", "06", "15", name))
		if err != nil {
			t.Fatal(err)
		}
		members := gzipMembers(t, raw)
		if len(members) != 1 {
			t.Fatalf("%s: expected 1 gzip member, got %d", name, len(members))
		}
	}

	results, err := c.Get(firstAt, secondAt)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].SomeInt != 10 || results[1].SomeInt != 11 {
		t.Fatalf("cross-hour compressed read mismatch: %+v", results)
	}
}

func Test_CorruptGzipFileReturnsError(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "2024", "06", "15", "10.cbor")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("this is not gzip"), 0o644); err != nil {
		t.Fatal(err)
	}

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	from := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	if _, err := c.Get(from, to); err == nil {
		t.Fatal("expected corrupt gzip data to fail the read")
	}
}

func Test_TruncatedGzipMemberReturnsError(t *testing.T) {
	tmpDir := t.TempDir()

	storedAt := time.Date(2024, 6, 15, 10, 5, 0, 0, time.UTC)
	compressed, err := encodeCompressed(Entry[testStruct]{
		Time: storedAt,
		Data: testStruct{SomeString: strings.Repeat("truncated-", 40), SomeInt: 1, SomeFloat: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(compressed) <= 18 {
		t.Fatalf("compressed record is too short to truncate: %d", len(compressed))
	}

	path := filepath.Join(tmpDir, "2024", "06", "15", "10.cbor")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, compressed[:len(compressed)-8], 0o644); err != nil {
		t.Fatal(err)
	}

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := c.Get(storedAt, storedAt); err == nil {
		t.Fatal("expected a truncated gzip member to fail the read")
	}
}

func Test_EmptyHourFileReadsNothing(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "2024", "06", "15", "10.cbor")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	c, err := Init[testStruct](Options{Path: tmpDir})
	if err != nil {
		t.Fatal(err)
	}

	from := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 15, 10, 59, 0, 0, time.UTC)
	results, err := c.Get(from, to)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("expected no records from an empty hour file, got %d", len(results))
	}
}
