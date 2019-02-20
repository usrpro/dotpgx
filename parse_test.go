package dotpgx

import (
	"strings"
	"testing"
)

const parseFile = "tests/parse.sql"

func compareQm(exp queryMap, got queryMap) []interface{} {
	msg := []interface{}{
		"Maps not same;\nExpected:\n",
		exp,
		"\nGot:\n",
		got,
	}

	if len(exp) == len(got) {
		for k, v := range exp {
			if got[k].sql != v.sql {
				return msg
			}
		}
	} else {
		return msg
	}
	return nil
}

var merge_expect queryMap = queryMap{
	"one":   &query{sql: "select 1;"},
	"two":   &query{sql: "select 2"},
	"three": &query{sql: "select 3"},
}

func TestMerge(t *testing.T) {
	qm := queryMap{
		"one": &query{sql: "select 1;"},
		"two": &query{sql: "select old;"},
	}
	qm2 := queryMap{
		"two":   &query{sql: "select 2"},
		"three": &query{sql: "select 3"},
	}
	qm = merge(qm, qm2)
	if msg := compareQm(merge_expect, qm); msg != nil {
		t.Fatal(msg...)
	}
}

func TestGetQuery(t *testing.T) {
	db := new(DB)
	db.qm = make(queryMap)
	err := db.ParseFiles(parseFile)
	if err != nil {
		t.Fatal("ParseFile err;", err)
	}
	q, err := db.qm.getQuery("two")
	if err != nil {
		t.Fatal("qm.getQuery error;", err)
	}
	exp := "select 2;"
	if q.sql != exp {
		t.Fatal("\nExpected:\n", exp, "\nGot:\n", q.sql)
	}
	q, err = db.qm.getQuery("none")
	if err == nil || q != nil {
		t.Fatal("Expected an error and empty sql;\n", "Got:", q.sql)
	}
}

func TestParseSqlErr(t *testing.T) {
	db := new(DB)
	db.qm = make(queryMap)
	err := db.ParseSql(strings.NewReader(""))
	if err == nil {
		t.Fatal("Expected a parse error")
	}
	err = db.ParseSql(
		strings.NewReader(`
			-- Nothing to parse here
		`),
	)
	if err == nil {
		t.Fatal("Expected a parse error")
	}
}

var parse_expect queryMap = queryMap{
	"one":    &query{sql: "select 1 from users where $1 = me;"},
	"two":    &query{sql: "select 2;"},
	"000000": &query{sql: "select 3;"},
	"000001": &query{sql: "select 4"},
	"five":   &query{sql: "select 5"},
}

// Tests ParseSql and ParseFile at once
func TestParseFiles(t *testing.T) {
	db := new(DB)
	db.qm = make(queryMap)
	err := db.ParseFiles(parseFile)
	if err != nil {
		t.Fatal("ParseFile err;", err)
	}
	if msg := compareQm(parse_expect, db.qm); msg != nil {
		t.Fatal(msg...)
	}
	err = db.ParseFiles()
	if err == nil {
		t.Fatal("Expected error for empty file list")
	}
	err = db.ParseFiles("nope")
	if err == nil {
		t.Fatal("Expected error for non-existing file")
	}
}

// This tests ParsePath and ParseFileBlob at once
// Parsing and merging is already tested, here we'll settle for the map size only
func TestParsePath(t *testing.T) {
	db := new(DB)
	db.qm = make(queryMap)
	err := db.ParsePath(queriesDir)
	if err != nil {
		t.Fatal("ParseFileGlob err;", err)
	}
	exp, got := 5, len(db.qm)
	if exp != got {
		t.Fatal("Expected", exp, "queries in the map; Got", got)
	}
}
