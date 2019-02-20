package dotpgx

import (
	"testing"
)

var tx *Tx

func cleanTx() {
	tx.Rollback()
	tx = nil
}

func TestTxBeginRollback(t *testing.T) {
	var err error
	tx, err = db.Begin()
	if err != nil {
		t.Fatal("Error in transaction begin", err)
	}
	defer cleanTx()
	err = tx.Rollback()
	if err != nil {
		t.Fatal("Error in transaction rollback", err)
	}
}

func TestTxQuery(t *testing.T) {
	var err error
	if tx == nil {
		tx, err = db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer cleanTx()
	}
	rows, err := tx.Query("find-peers-by-email", "foo@bar.com")
	if err != nil {
		t.Fatal("Error in query execution", err)
	}
	got, err := rowScan(rows)
	if err != nil {
		t.Fatal("rowSacan error;", err)
	}
	exp := peers[:2]
	if msg := comparePeers(exp, got); msg != nil {
		t.Fatal(msg...)
	}
}

func TestTxQueryRow(t *testing.T) {
	var err error
	if tx == nil {
		tx, err = db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer cleanTx()
	}
	row, err := tx.QueryRow("find-one-peer-by-email", "bar@foo.com")
	if err != nil {
		t.Fatal("Error in query execution", err)
	}
	var got peer
	if err = row.Scan(&got.name, &got.email); err != nil {
		t.Fatal("Error in row scan", err)
	}
	if !comparePeer(peers[2], got) {
		t.Fatal(
			"Peers not same\nExpected:\n",
			peers[2],
			"\nGot:\n",
			got,
		)
	}
}

func TestTxExecCommit(t *testing.T) {
	var err error
	if tx == nil {
		tx, err = db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer cleanTx()
	}
	p := peers[4]
	if _, err := tx.Exec("create-peer", p.name, p.email); err != nil {
		t.Fatal("Error inserting peer;", p, err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal("Error on commit", p, err)
	}
}

func TestTxPrepare(t *testing.T) {
	var err error
	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer cleanTx()
	name := "find-peers-by-email"
	if _, err := tx.Prepare(name); err != nil {
		t.Fatal("Error in prepare statement", err)
	}
	// Check if the query are indeed prepared
	if !db.qm[name].isPrepared() {
		t.Fatal("Query not prepared:", name)
	}

	t.Run("Prepared TX query", TestTxQuery)
	// Re-parse to test auto-clear
	err = db.ParsePath("tests/queries")
	if err != nil {
		t.Fatal(err)
	}
}
