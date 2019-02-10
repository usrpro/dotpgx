package dotpgx

import (
	"testing"
)

func compareQm(exp queryMap, got queryMap) []interface{} {
	msg := []interface{}{
		"Maps not same;\nExpected:\n",
		exp,
		"\nGot:\n",
		got,
	}

	if len(exp) == len(got) {
		for k, v := range exp {
			if got[k] != v {
				return msg
			}
		}
	} else {
		return msg
	}
	return nil
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
	if msg := compareQm(merge_expect, qm); msg != nil {
		t.Error(msg...)
	}
}

func TestGetQuery(t *testing.T) {
	db := new(DB)
	err := db.ParseFiles("parse_test.sql")
	if err != nil {
		t.Error("ParseFile err;", err)
		return
	}
	sql, err := db.qm.getQuery("two")
	if err != nil {
		t.Error("qm.getQuery error;", err)
		return
	}
	exp := "select 2;"
	if sql != exp {
		t.Error("\nExpected:\n", exp, "\nGot:\n", sql)
	}
	sql, err = db.qm.getQuery("none")
	if err == nil || len(sql) != 0 {
		t.Error("Expected an error and empty sql;\n", "Got:", sql)
	}
}

var parse_expect queryMap = queryMap{
	"one":    "select 1 from users where $1 = me;",
	"two":    "select 2;",
	"000000": "select 3;",
	"000001": "select 4",
	"five":   "select 5",
}

// Tests ParseSql and ParseFile at once
func TestParseFiles(t *testing.T) {
	db := new(DB)
	err := db.ParseFiles("parse_test.sql")
	if err != nil {
		t.Error("ParseFile err;", err)
		return
	}
	if msg := compareQm(parse_expect, db.qm); msg != nil {
		t.Error(msg...)
	}
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
