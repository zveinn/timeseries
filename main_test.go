package timeseries

import "testing"

type testStruct struct {
	SomeString string
	SomeInt    int
	SomeFloat  float64
}

func Test_init(t *testing.T) {
	c, err := Init[testStruct](Options{
		Debug:       true,
		PrintMemory: true,
	})
	if c == nil || err != nil {
		t.Fatal("unable to init")
	}
}
