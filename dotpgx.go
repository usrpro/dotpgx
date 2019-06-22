// Package dotpgx creates a connection pool, parses and executes queries.
package dotpgx

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/log/log15adapter"
	log "gopkg.in/inconshreveable/log15.v2"
)

// DB represents the database connection pool and parsed queries.
type DB struct {
	// Pool allows direct access to the underlying *pgx.ConnPool
	Pool *pgx.ConnPool
	qm   queryMap
	qn   int // Incremented value for unamed queries
}

/*
New configures and creates a database connection pool
It returns a pointer to the Database object.
It returns an error only when the connection pool cannot be set-up.

An example config object would look like:

	conf := pgx.ConnPoolConfig{
		ConnConfig: pgx.ConnConfig{
			Host:     pgHost,
			User:     pgUser,
			Database: pgDatabase,
			Logger:   logger,
		},
		MaxConnections: 50,
		AfterConnect:   sqlPrepare,
	}

Most arguments are optional. If no logger is specified,
one will get apointed automatically.
*/
func New(conf pgx.ConnPoolConfig) (db *DB, err error) {
	if conf.Logger == nil {
		conf.Logger = log15adapter.NewLogger(log.New("module", "pgx"))
	}
	pool, err := pgx.NewConnPool(conf)
	if err != nil {
		log.Crit("Unable to create connection pool", "error", err)
		return
	}
	db = &DB{
		Pool: pool,
		qm:   make(queryMap),
	}
	return
}

// HasQueries returns true if the are queries in the map.
// False in case of nil map or 0 queries.
func (db *DB) HasQueries() bool {
	return db.qm != nil && len(db.qm) > 0
}

// List of all registered query names, sorted
func (db *DB) List() (index []string) {
	return db.qm.sort()
}

// Prepare a sql statement identified by name.
func (db *DB) Prepare(name string) (*pgx.PreparedStatement, error) {
	q, err := db.qm.getQuery(name)
	if err != nil {
		return nil, err
	}
	q.ps, err = db.Pool.Prepare(name, q.sql)
	if err != nil {
		return nil, err
	}
	return q.ps, nil
}

// PrepareAll prepares all registered queries. It returns an error
// when one of the queries failed to prepare. However, it will not
// abort in such case and attempts to prepare the remaining statements.
func (db *DB) PrepareAll() (ps []*pgx.PreparedStatement, err error) {
	msg := []string{}
	for name, query := range db.qm {
		p, e := db.Prepare(name)
		if e != nil {
			m := []string{
				"Error in preparing statement:",
				name,
				"; With query:",
				query.sql,
			}
			msg = append(msg, strings.Join(m, " "))
		} else {
			ps = append(ps, p)
		}
	}
	if len(msg) > 0 {
		err = errors.New(strings.Join(msg, "\n"))
	}
	return
}

// Query runs the sql indentified by name. Return a row set.
func (db *DB) Query(name string, args ...interface{}) (*pgx.Rows, error) {
	q, err := db.qm.getQuery(name)
	if err != nil {
		return nil, err
	}
	return db.Pool.Query(q.getSQL(), args...)
}

// QueryRow runs the sql identified by name. It returns a single row.
// Not that an error is only returned if the query is not defined.
// A query error is defered untill row.Scan is run. See pgx docs for more info.
func (db *DB) QueryRow(name string, args ...interface{}) (*pgx.Row, error) {
	q, err := db.qm.getQuery(name)
	if err != nil {
		return nil, err
	}
	return db.Pool.QueryRow(q.getSQL(), args...), nil
}

// Exec runs the sql identified by name. Returns the result of the exec or an error.
func (db *DB) Exec(name string, args ...interface{}) (pgx.CommandTag, error) {
	q, err := db.qm.getQuery(name)
	if err != nil {
		return "", err
	}
	return db.Pool.Exec(q.getSQL(), args...)
}

// DropQuery removes a query form the Map.
// It calls Pgx Deallocate if the query was a prepared statement.
// An error is returned only when deallocating fails.
// Regardless of an error, the query will be dropped from the map.
func (db *DB) DropQuery(name string) (err error) {
	if db.qm[name].isPrepared() {
		err = db.Pool.Deallocate(name)
	}
	mutex.Lock()
	delete(db.qm, name)
	mutex.Unlock()
	return
}

// ClearMap clears the query map and sets the internal incremental counter to 0.
// Use this before you want to load a fresh set of queries, keeping the connection pool open.
// An error is only returned if one or more prepared statements failed to deallocate.
// It does not abbort on error and continues to (attempt) the clear the remaining queries.
func (db *DB) ClearMap() (err error) {
	var msg []string
	for name := range db.qm {
		err := db.DropQuery(name)
		if err != nil {
			msg = append(msg, fmt.Sprint(err))
		}
	}
	db.qn = 0
	if len(msg) > 0 {
		err = errors.New(strings.Join(msg, "\n"))
	}
	return
}

// Close cleans up the mapped queries and closes the pgx connection pool.
// It is safe to call close multiple times.
func (db *DB) Close() {
	// Possible Deaollocate errors ignored, we are going to close the connnection anyway.
	db.ClearMap()
	db.Pool.Close()
}
