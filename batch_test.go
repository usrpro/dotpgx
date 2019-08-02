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

func testBatch(b *Batch, t *testing.T) {
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

func TestBatch(t *testing.T) {
	db, err := New(Default.connPoolConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.ParseFiles("tests/batch.sql"); err != nil {
		t.Fatal(err)
	}
	// Plain batch
	b := db.BeginBatch()
	testBatch(b, t)
	// Batch inside transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	b = tx.BeginBatch()
	testBatch(b, t)
}
