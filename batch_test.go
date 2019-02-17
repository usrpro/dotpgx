package dotpgx

import (
	"reflect"
	"testing"
)

var exp = []string{
	"Hello",
	"World!",
	"Spanac",
	"Eggs",
}

func TestBatch(t *testing.T) {
	db, err := New(conf)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.ParseFiles("tests/batch.sql"); err != nil {
		t.Fatal(err)
	}
	b := db.BeginBatch()
	b.QueueAll()
	if err := b.Send(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		p, err := b.ExecResults()
		if err != nil {
			t.Fatal(err)
		}
		ra := p.RowsAffected()
		if i > 0 && ra != 1 {
			t.Fatal("Expected 1 affected row, got", ra)
		}
	}
	rows, err := b.QueryResults()
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	var s string
	for rows.Next() {
		rows.Scan(&s)
		got = append(got, s)
	}
	if !reflect.DeepEqual(exp, got) {
		t.Fatal("QueryResults\nExpected:\n", exp, "\nGot:\n", got)
	}
	row := b.QueryRowResults()
	if err := row.Scan(&s); err != nil {
		t.Fatal(err)
	}
	es := "Spanac"
	if s != es {
		t.Fatal("QueryRowResults\nExpected:\n", es, "\nGot:\n", s)
	}

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}
