package tracecache

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestWrap(t *testing.T) {
	var traceResult = struct {
		A string `json:"a"`
		B int    `json:"b"`
	}{
		A: "hello",
		B: 123,
	}
	bt, _ := json.Marshal(traceResult)
	raw := json.RawMessage{}
	err := raw.UnmarshalJSON(bt)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(raw)
	fmt.Println(string(b), err)
}
