// +build all parse

package dotpgx

import (
	"testing"
)

func mapCompare(exp queryMap, got queryMap, t *testing.T) {
	msg := []interface{}{ //ouch
		"Maps not same;\nExpected:\n",
		exp,
		"\nGot:\n",
		got,
	}

	if len(exp) == len(got) {
		for k, v := range exp {
			if got[k] != v {
				t.Error(msg...)
				return
			}
		}
	} else {
		t.Error(msg...)
	}
}

var merge_expect queryMap = queryMap{
	"one":   "select 1;",
	"two":   "select 2",
	"three": "select 3",
}

func TestMerge(t *testing.T) {
	qm := queryMap{
		"one": "select 1;",
		"two": "select old;",
	}
	qm2 := queryMap{
		"two":   "select 2",
		"three": "select 3",
	}
	qm = merge(qm, qm2)
	mapCompare(merge_expect, qm, t)
}

var parse_expect queryMap = queryMap{
	"one":    "select 1 from users where $1 = me;",
	"two":    "select 2;",
	"000000": "select 3;",
	"000001": "select 4",
	"five":   "select 5",
}

// Tests ParseSql and ParseFile at once
func TestParseFile(t *testing.T) {
	db := new(DB)
	err := db.ParseFiles("parse_test.sql")
	if err != nil {
		t.Error("ParseFile err;", err)
		return
	}
	mapCompare(parse_expect, db.qm, t)
}

// This tests ParsePath and ParseFileBlob at once
// Parsing and merging is already tested, here we'll settle for the map size only
func TestParsePath(t *testing.T) {
	db := new(DB)
	err := db.ParsePath("glob_test")
	if err != nil {
		t.Error("ParseFileGlob err;", err)
		return
	}
	exp, got := 5, len(db.qm)
	if exp != got {
		t.Error("Expected", exp, "queries in the map; Got", got)
	}
}