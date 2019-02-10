package dotpgx

import (
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx"
)

var conf = pgx.ConnPoolConfig{
	ConnConfig: pgx.ConnConfig{
		Host:     "/run/postgresql",
		User:     "postgres",
		Database: "dotpgx_test",
	},
	MaxConnections: 5,
}

var db *DB

type peer struct {
	name  string
	email string
}

var peers = []peer{
	{"Foo Bar", "foo@bar.com"},
	{"Double Trouble", "foo@bar.com"},
	{"Lonely Ranger", "bar@foo.com"},
	{"Mickey Mouse", "mickey@disney.com"},
	{"Donald Duck", "donald@disney.com"},
}

func clean() {
	if _, err := db.Exec("drop-peers-table"); err != nil {
		fmt.Println("Unable to drop peers table", err)
	}
	db.Close()
}

func comparePeer(a peer, b peer) bool {
	return a.name == b.name && a.email == b.email
}

func rowScan(rows *pgx.Rows) (peers []peer, err error) {
	for rows.Next() {
		var p peer
		if err = rows.Scan(&p.name, &p.email); err != nil {
			return
		}
		peers = append(peers, p)
	}
	return
}

func comparePeers(exp []peer, got []peer) []interface{} {
	msg := []interface{}{ //ouch
		"Peers slice not same;\nExpected:\n",
		exp,
		"\nGot:\n",
		got,
	}
	if len(exp) == len(got) {
		for k, e := range exp {
			if !comparePeer(e, got[k]) {
				return msg
			}
		}
	} else {
		return msg
	}
	return nil
}
func TestMain(m *testing.M) {
	f := func() int {
		var err error
		db, err = New(conf)
		if err != nil {
			panic(err)
		}
		err = db.ParsePath("glob_test")
		if err != nil {
			panic(err)
		}
		if db.qm["drop-peers-table"] == "" {
			panic("Cleanup query not loaded, aborting")
		}
		defer clean()
		if _, err := db.Exec("create-peers-table"); err != nil {
			panic(err)
		}
		for _, p := range peers[:3] {
			if _, err := db.Exec("create-peer", p.name, p.email); err != nil {
				panic(err)
			}
		}
		return m.Run()
	}
	os.Exit(f())
}

func TestExec(t *testing.T) {
	p := peers[3]
	if _, err := db.Exec("create-peer", p.name, p.email); err != nil {
		t.Error("Error inserting peer;", p, err)
		return
	}
}

func TestQuery(t *testing.T) {
	rows, err := db.Query("find-peers-by-email", "foo@bar.com")
	if err != nil {
		t.Error("Error in query execution", err)
		return
	}
	got, err := rowScan(rows)
	if err != nil {
		t.Error("rowScan error;", err)
	}
	exp := peers[:2]
	if msg := comparePeers(exp, got); msg != nil {
		t.Error(msg...)
	}
}

func TestQueryRow(t *testing.T) {
	row, err := db.QueryRow("find-one-peer-by-email", "bar@foo.com")
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
