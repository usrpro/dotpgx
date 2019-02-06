// +build all integration

package dotpgx

import (
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
	{"Mickey Mouse", "mickey@disney.com"},
	{"Donald Duck", "donald@disney.com"},
	{"Foo Bar", "foo@bar.com"},
	{"Double Trouble", "foo@bar.com"},
}

func clean() {
	defer db.Close()
	if _, err := db.Exec("drop-peers-table"); err != nil {
		panic(err)
	}
	// Check if we can safely close multiple times
	db.Close()
	db.Close()
}
func TestMain(m *testing.M) {
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
	e := m.Run()
	clean()
	os.Exit(e)
}

func TestExec(t *testing.T) {
	if _, err := db.Exec("create-peers-table"); err != nil {
		t.Error("Error creating peers table", err)
		return
	}
	for _, u := range peers {
		if _, err := db.Exec("create-peer", u.name, u.email); err != nil {
			t.Error("Error inserting peer;", u, err)
			return
		}
	}
}

func TestQuery(t *testing.T) {
	rows, err := db.Query("find-peers-by-email", "foo@bar.com")
	if err != nil {
		t.Error("Error in query execution", err)
		return
	}
	var name, email string
	for rows.Next() {
		if err = rows.Scan(&name, &email); err != nil {
			t.Error("Error in row scan", err)
			return
		}
	}
}

func TestQueryRow(t *testing.T) {
	row, err := db.QueryRow("find-one-peer-by-email", "mickey@disney.com")
	if err != nil {
		t.Error("Error in query execution", err)
		return
	}
	var name, email string
	if err = row.Scan(&name, &email); err != nil {
		t.Error("Error in row scan", err)
		return
	}
}
