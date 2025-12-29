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
