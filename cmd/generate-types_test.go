package main

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
)

func TestVersionRangeUnmarshalJSON(t *testing.T) {
	tests := []struct {
		versionRange string
		expected     VersionRange
	}{
		{
			versionRange: `"0+"`,
			expected: VersionRange{
				Min: 0,
				Max: math.MaxInt,
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.versionRange, func(t *testing.T) {
			var vr VersionRange
			err := json.Unmarshal([]byte(test.versionRange), &vr)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(test.expected, vr) {
				t.Fatalf("unexpected result\nexpected: %#v\ngot: %#v", test.expected, vr)
			}
		})
	}
}
