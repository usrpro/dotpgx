package dotpgx

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx"
)

const queriesDir = "tests/queries"

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
		err = db.ParsePath(queriesDir)
		if err != nil {
			panic(err)
		}
		fmt.Println(db.qm)
		if db.qm["drop-peers-table"] == nil {
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

func TestNewHasListClearClose(t *testing.T) {
	bc := pgx.ConnPoolConfig{
		ConnConfig: pgx.ConnConfig{
			Host:     "nohost",
			User:     "postgres",
			Database: "dotpgx_test",
		},
		MaxConnections: 5,
	}
	if cp, err := New(bc); cp != nil || err == nil {
		t.Fatal("No error generated in new")
	}
	// Create new connection in the local scope, so we can close it whithout affecting other tests.
	cp, err := New(conf)
	if err != nil {
		t.Fatal(err)
	}
	err = cp.ParseFiles("tests/list.sql")
	if err != nil {
		t.Fatal(err)
	}
	if !cp.HasQueries() {
		t.Fatal("HasQueries returned false")
	}
	exp := []string{
		"000000",
		"000001",
		"000002",
	}
	got := cp.List()
	if !reflect.DeepEqual(exp, got) {
		t.Fatal("Lists not equal\nExpected:\n", exp, "\nGot:\n", got)
	}
	err = cp.ClearMap()
	if err != nil {
		t.Fatal(err)
	}
	if len(cp.qm) > 0 && cp.qn != 0 {
		t.Fatal("Failed to clear the query map:", cp.qm, cp.qn)
	}
	// See of we can parse again
	err = cp.ParsePath(queriesDir)
	if err != nil {
		t.Fatal(err)
	}
	// Test multiple close
	cp.Close()
	cp.Close()
}

func TestQuery(t *testing.T) {
	rows, err := db.Query("find-peers-by-email", "foo@bar.com")
	if err != nil {
		t.Fatal("Error in query execution", err)
	}
	got, err := rowScan(rows)
	if err != nil {
		t.Fatal("rowScan error;", err)
	}
	exp := peers[:2]
	if msg := comparePeers(exp, got); msg != nil {
		t.Fatal(msg...)
	}
}

func TestQueryRow(t *testing.T) {
	row, err := db.QueryRow("find-one-peer-by-email", "bar@foo.com")
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

func TestExec(t *testing.T) {
	p := peers[3]
	if _, err := db.Exec("create-peer", p.name, p.email); err != nil {
		t.Fatal("Error inserting peer;", p, err)
	}
}

func TestPrepare(t *testing.T) {
	if _, err := db.Prepare("find-peers-by-email"); err != nil {
		t.Fatal("Error in prepare statement", err)
	}
	// Test the error in PrepareAll
	m := []string{
		"Error in preparing statement:",
		"spanac",
		"; With query:",
		"spanac $?;",
	}
	exp := strings.Join(m, " ")
	r := strings.NewReader("--name: spanac\nspanac $?;")
	if err := db.ParseSql(r); err != nil {
		t.Fatal(err)
	}
	_, err := db.PrepareAll()
	if err == nil || fmt.Sprint(err) != exp {
		t.Fatal("Incorrect error condition from PrepareAll\nExpected:\n", exp, "\nGot:\n", err)
	}
	if err := db.DropQuery("spanac"); err != nil {
		t.Fatal(err)
	}
	// Check if all the queries are indeed prepared
	for name, query := range db.qm {
		if !query.isPrepared() {
			t.Fatal("Query not prepared:", name)
		}
	}

	t.Run("Prepared query", TestQuery)
	t.Run("Prepared query row", TestQueryRow)
	t.Run("Prepared exec", TestExec)
	// Re-parse to test auto-clear
	err = db.ParsePath(queriesDir)
	if err != nil {
		t.Fatal(err)
	}
}
