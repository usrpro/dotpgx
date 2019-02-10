package dotpgx

import (
	"testing"
)

func TestTxBeginRollback(t *testing.T) {
	tx, err := db.Begin()
	if err != nil {
		t.Error("Error in transaction begin", err)
		return
	}
	err = tx.Rollback()
	if err != nil {
		t.Error("Error in transaction rollback", err)
		return
	}
}
func TestTxExecCommit(t *testing.T) {
	tx, err := db.Begin()
	if err != nil {
		t.Error(err)
		return
	}
	defer tx.Rollback()
	p := peers[4]
	if _, err := tx.Exec("create-peer", p.name, p.email); err != nil {
		t.Error("Error inserting peer;", p, err)
		return
	}
	if err = tx.Commit(); err != nil {
		t.Error("Error on commit", p, err)
	}
}

func TestTxQuery(t *testing.T) {
	tx, err := db.Begin()
	if err != nil {
		t.Error(err)
		return
	}
	defer tx.Rollback()
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
	tx, err := db.Begin()
	if err != nil {
		t.Error(err)
		return
	}
	defer tx.Rollback()
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
