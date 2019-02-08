// Package dotpgx creates a connection pool, parses and executes queries.
package dotpgx

import (
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
func New(conf pgx.ConnPoolConfig) (*DB, error) {
	if conf.Logger == nil {
		conf.Logger = log15adapter.NewLogger(log.New("module", "pgx"))
	}
	pool, err := pgx.NewConnPool(conf)
	if err != nil {
		log.Crit("Unable to create connection pool", "error", err)
		return nil, err
	}
	return &DB{Pool: pool}, nil
}

// Prepare a sql statement identified by name.
func (db *DB) Prepare(name string) (*pgx.PreparedStatement, error) {
	sql, err := db.qm.getQuery(name)
	if err != nil {
		return nil, err
	}
	return db.Pool.Prepare(name, sql)
}

// Query runs the sql indentified by name. Return a row set.
func (db *DB) Query(name string, args ...interface{}) (*pgx.Rows, error) {
	sql, err := db.qm.getQuery(name)
	if err != nil {
		return nil, err
	}
	return db.Pool.Query(sql, args...)
}

// QueryRow runs the sql identified by name. It returns a single row.
// Not that an error is only returned if the query is not defined.
// A query error is defered untill row.Scan is run. See pgx docs for more info.
func (db *DB) QueryRow(name string, args ...interface{}) (*pgx.Row, error) {
	sql, err := db.qm.getQuery(name)
	if err != nil {
		return nil, err
	}
	return db.Pool.QueryRow(sql, args...), nil
}

// Exec runs the sql identified by name. Returns the result of the exec or an error.
func (db *DB) Exec(name string, args ...interface{}) (pgx.CommandTag, error) {
	sql, err := db.qm.getQuery(name)
	if err != nil {
		return "", err
	}
	return db.Pool.Exec(sql, args...)
}

// ClearSql clears the query map and sets the internal incremental counter to 0.
// Use this before you want to load a fresh set of queries, keeping the connection pool open.
func (db *DB) ClearSql() {
	db.qm = nil
	db.qn = 0
}

// Close cleans up the mapped queries and closes the pgx connection pool.
// It is safe to call close multiple times.
func (db *DB) Close() {
	db.ClearSql()
	db.Pool.Close()
}
