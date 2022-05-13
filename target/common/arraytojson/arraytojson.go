package arraytojson

import (
	"bytes"
	"encoding/json"

	"github.com/jackc/pgtype"
)

func PGArrayToJSON(src string) (string, error) {
	if src == "" {
		return "", nil
	}
	arr, err := pgtype.ParseUntypedTextArray(src)
	if err != nil {
		return "", err
	}
	return textArrayToJSON(arr)
}

// From: https://github.com/jackc/pgtype/pull/37/files
func textArrayToJSON(src *pgtype.UntypedTextArray) (string, error) {
	// avoid default json.Marshal behavior for nil Element slices
	if src.Elements == nil {
		return "[]", nil
	}
	// each dimensions, marshal json and insert into result array
	if len(src.Dimensions) == 1 {
		out, err := json.Marshal(src.Elements)
		if err != nil {
			return "", err
		}
		return string(out), nil
	}

	dimElemCounts := getDimElemCounts(src.Dimensions)
	var buf bytes.Buffer
	for i, elem := range src.Elements {
		if i > 0 {
			buf.WriteString(",")
		}
		for _, dec := range dimElemCounts {
			if i%dec == 0 {
				buf.WriteString("[")
			}
		}

		eb, err := json.Marshal(elem)
		if err != nil {
			return "", err
		}
		buf.Write(eb)

		for _, dec := range dimElemCounts {
			if (i+1)%dec == 0 {
				buf.WriteString("]")
			}
		}
	}
	return buf.String(), nil
}

// multidimensional array helper
// dimElemCounts is the multiples of elements that each array lies on. For
// example, a single dimension array of length 4 would have a dimElemCounts of
// [4]. A multi-dimensional array of lengths [3,5,2] would have a
// dimElemCounts of [30,10,2]. This is used to simplify when to render a '['
// or ']'.
func getDimElemCounts(dims []pgtype.ArrayDimension) []int {
	dimElemCounts := make([]int, len(dims))
	dimElemCounts[len(dims)-1] = int(dims[len(dims)-1].Length)
	for i := len(dims) - 2; i > -1; i-- {
		dimElemCounts[i] = int(dims[i].Length) * dimElemCounts[i+1]
	}
	return dimElemCounts
}
