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
		t.Error("Error in transaction begin", err)
		return
	}
	defer cleanTx()
	err = tx.Rollback()
	if err != nil {
		t.Error("Error in transaction rollback", err)
		return
	}
}

func TestTxQuery(t *testing.T) {
	var err error
	if tx == nil {
		tx, err = db.Begin()
		if err != nil {
			t.Error(err)
			return
		}
		defer cleanTx()
	}
	rows, err := tx.Query("find-peers-by-email", "foo@bar.com")
	if err != nil {
		t.Error("Error in query execution", err)
		return
	}
	got, err := rowScan(rows)
	if err != nil {
		t.Error("rowSacan error;", err)
	}
	exp := peers[:2]
	if msg := comparePeers(exp, got); msg != nil {
		t.Error(msg...)
	}
}

func TestTxQueryRow(t *testing.T) {
	var err error
	if tx == nil {
		tx, err = db.Begin()
		if err != nil {
			t.Error(err)
			return
		}
		defer cleanTx()
	}
	row, err := tx.QueryRow("find-one-peer-by-email", "bar@foo.com")
	if err != nil {
		t.Error("Error in query execution", err)
		return
	}
	var got peer
	if err = row.Scan(&got.name, &got.email); err != nil {
		t.Error("Error in row scan", err)
		return
	}
	if !comparePeer(peers[2], got) {
		t.Error(
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
			t.Error(err)
			return
		}
		defer cleanTx()
	}
	p := peers[4]
	if _, err := tx.Exec("create-peer", p.name, p.email); err != nil {
		t.Error("Error inserting peer;", p, err)
		return
	}
	if err = tx.Commit(); err != nil {
		t.Error("Error on commit", p, err)
	}
}

func TestTxPrepare(t *testing.T) {
	var err error
	tx, err = db.Begin()
	if err != nil {
		t.Error(err)
		return
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
	err = db.ParsePath("glob_test")
	if err != nil {
		t.Fatal(err)
	}
}
