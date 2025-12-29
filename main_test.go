package timeseries

import (
	"os"
	"path/filepath"
	"testing"
	"time"
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

	expectedPath := filepath.Join(tmpDir, "2024", "06", "15", "10", "30.cbor")
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
	err = c.Find(baseTime.Add(-time.Minute), baseTime.Add(5*time.Minute), func(tm time.Time, data testStruct) {
		count++
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

	err = c.Delete(baseTime, baseTime.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	results, err := c.Get(baseTime.Add(-time.Minute), baseTime.Add(5*time.Minute))
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

	err = c.Find(baseTime, baseTime.Add(4*time.Minute), func(tm time.Time, data testStruct) {
		times = append(times, tm)
		values = append(values, data.SomeInt)
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
