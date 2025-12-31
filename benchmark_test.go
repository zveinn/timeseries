package timeseries

import (
	"os"
	"testing"
	"time"
)

const benchmarkItemCount = 100000

var (
	benchTmpDir  string
	benchClient  *Client[benchStruct]
	benchBase    time.Time
	benchEndTime time.Time
)

type benchStruct struct {
	ID    int
	Name  string
	Value float64
}

func TestMain(m *testing.M) {
	code := m.Run()
	// Cleanup after all tests/benchmarks
	if benchTmpDir != "" {
		os.RemoveAll(benchTmpDir)
	}
	os.Exit(code)
}

// BenchmarkA_Store runs first - stores 100,000 items
func BenchmarkA_Store(b *testing.B) {
	var err error
	benchTmpDir, err = os.MkdirTemp("", "bench-timeseries-*")
	if err != nil {
		b.Fatal(err)
	}

	benchClient, err = Init[benchStruct](Options{Path: benchTmpDir})
	if err != nil {
		os.RemoveAll(benchTmpDir)
		b.Fatal(err)
	}

	benchBase = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	benchEndTime = benchBase.Add(time.Duration(benchmarkItemCount-1) * time.Minute)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < benchmarkItemCount; j++ {
			err = benchClient.Store(benchBase.Add(time.Duration(j)*time.Minute), benchStruct{
				ID:    j,
				Name:  "benchmark",
				Value: float64(j),
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkB_Get runs second - retrieves all 100,000 items
func BenchmarkB_Get(b *testing.B) {
	if benchClient == nil {
		b.Skip("BenchmarkA_Store must run first")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := benchClient.Get(benchBase, benchEndTime)
		if err != nil {
			b.Fatal(err)
		}
		if len(results) != benchmarkItemCount {
			b.Fatalf("expected %d results, got %d", benchmarkItemCount, len(results))
		}
	}
}

// BenchmarkC1_Find_1Minute - searches within 1 minute window (1 item)
func BenchmarkC1_Find_1Minute(b *testing.B) {
	if benchClient == nil {
		b.Skip("BenchmarkA_Store must run first")
	}

	// 1 minute window = 1 item (item 0)
	windowEnd := benchBase
	targetID := 0

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var found *benchStruct
		err := benchClient.Find(benchBase, windowEnd, func(t time.Time, data benchStruct) bool {
			if data.ID == targetID {
				found = &data
			}
			return true
		})
		if err != nil {
			b.Fatal(err)
		}
		if found == nil || found.ID != targetID {
			b.Fatalf("expected to find item with ID %d", targetID)
		}
	}
}

// BenchmarkC2_Find_1Hour - searches within 1 hour window (60 items)
func BenchmarkC2_Find_1Hour(b *testing.B) {
	if benchClient == nil {
		b.Skip("BenchmarkA_Store must run first")
	}

	// 1 hour = 60 minutes = 60 items
	itemCount := 60
	windowEnd := benchBase.Add(time.Duration(itemCount-1) * time.Minute)
	targetID := itemCount - 1

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var found *benchStruct
		err := benchClient.Find(benchBase, windowEnd, func(t time.Time, data benchStruct) bool {
			if data.ID == targetID {
				found = &data
			}
			return true
		})
		if err != nil {
			b.Fatal(err)
		}
		if found == nil || found.ID != targetID {
			b.Fatalf("expected to find item with ID %d", targetID)
		}
	}
}

// BenchmarkC3_Find_1Day - searches within 1 day window (1440 items)
func BenchmarkC3_Find_1Day(b *testing.B) {
	if benchClient == nil {
		b.Skip("BenchmarkA_Store must run first")
	}

	// 1 day = 24 hours = 1440 minutes = 1440 items
	itemCount := 24 * 60
	windowEnd := benchBase.Add(time.Duration(itemCount-1) * time.Minute)
	targetID := itemCount - 1

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var found *benchStruct
		err := benchClient.Find(benchBase, windowEnd, func(t time.Time, data benchStruct) bool {
			if data.ID == targetID {
				found = &data
			}
			return true
		})
		if err != nil {
			b.Fatal(err)
		}
		if found == nil || found.ID != targetID {
			b.Fatalf("expected to find item with ID %d", targetID)
		}
	}
}

// BenchmarkC4_Find_1Week - searches within 1 week window (10080 items)
func BenchmarkC4_Find_1Week(b *testing.B) {
	if benchClient == nil {
		b.Skip("BenchmarkA_Store must run first")
	}

	// 1 week = 7 days = 10080 minutes = 10080 items
	itemCount := 7 * 24 * 60
	windowEnd := benchBase.Add(time.Duration(itemCount-1) * time.Minute)
	targetID := itemCount - 1

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var found *benchStruct
		err := benchClient.Find(benchBase, windowEnd, func(t time.Time, data benchStruct) bool {
			if data.ID == targetID {
				found = &data
			}
			return true
		})
		if err != nil {
			b.Fatal(err)
		}
		if found == nil || found.ID != targetID {
			b.Fatalf("expected to find item with ID %d", targetID)
		}
	}
}

// BenchmarkC5_Find_1Month - searches within 1 month window (43200 items)
func BenchmarkC5_Find_1Month(b *testing.B) {
	if benchClient == nil {
		b.Skip("BenchmarkA_Store must run first")
	}

	// 1 month = 30 days = 43200 minutes = 43200 items
	itemCount := 30 * 24 * 60
	windowEnd := benchBase.Add(time.Duration(itemCount-1) * time.Minute)
	targetID := itemCount - 1

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var found *benchStruct
		err := benchClient.Find(benchBase, windowEnd, func(t time.Time, data benchStruct) bool {
			if data.ID == targetID {
				found = &data
			}
			return true
		})
		if err != nil {
			b.Fatal(err)
		}
		if found == nil || found.ID != targetID {
			b.Fatalf("expected to find item with ID %d", targetID)
		}
	}
}

// BenchmarkC6_Find_Full - searches entire dataset for item at end (100000 items)
func BenchmarkC6_Find_Full(b *testing.B) {
	if benchClient == nil {
		b.Skip("BenchmarkA_Store must run first")
	}

	// Full dataset = 100000 items
	targetID := benchmarkItemCount - 1

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var found *benchStruct
		err := benchClient.Find(benchBase, benchEndTime, func(t time.Time, data benchStruct) bool {
			if data.ID == targetID {
				found = &data
			}
			return true
		})
		if err != nil {
			b.Fatal(err)
		}
		if found == nil || found.ID != targetID {
			b.Fatalf("expected to find item with ID %d", targetID)
		}
	}
}

// BenchmarkD_GetSubset runs fourth - retrieves 10% of stored items
func BenchmarkD_GetSubset(b *testing.B) {
	if benchClient == nil {
		b.Skip("BenchmarkA_Store must run first")
	}

	subsetSize := benchmarkItemCount / 10
	startTime := benchBase.Add(time.Duration(benchmarkItemCount/2-subsetSize/2) * time.Minute)
	endTime := startTime.Add(time.Duration(subsetSize-1) * time.Minute)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := benchClient.Get(startTime, endTime)
		if err != nil {
			b.Fatal(err)
		}
		if len(results) != subsetSize {
			b.Fatalf("expected %d results, got %d", subsetSize, len(results))
		}
	}
}

// BenchmarkE_CacheLookup runs fifth - pure cache lookup performance
func BenchmarkE_CacheLookup(b *testing.B) {
	if benchClient == nil {
		b.Skip("BenchmarkA_Store must run first")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < benchmarkItemCount; j++ {
			benchClient.getCache(benchBase.Add(time.Duration(j) * time.Minute))
		}
	}
}

// BenchmarkF_CacheInit runs sixth - reinitialize cache from existing files
func BenchmarkF_CacheInit(b *testing.B) {
	if benchTmpDir == "" {
		b.Skip("BenchmarkA_Store must run first")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Init[benchStruct](Options{Path: benchTmpDir})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkZ_Delete runs last - deletes all 100,000 items
func BenchmarkZ_Delete(b *testing.B) {
	if benchClient == nil {
		b.Skip("BenchmarkA_Store must run first")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := benchClient.Delete(benchBase, benchEndTime)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	// Cleanup
	os.RemoveAll(benchTmpDir)
	benchTmpDir = ""
	benchClient = nil
}
