package dotpgx

import (
	"github.com/jackc/pgx"
)

type Tx struct {
	Ptx *pgx.Tx
	qm  queryMap
}

// Begin a transaction
func (db *DB) Begin() (tx *Tx, err error) {
	ptx, err := db.Pool.Begin()
	if err != nil {
		return
	}
	tx = &Tx{
		Ptx: ptx,
		qm:  db.qm,
	}
	return
}

// Rollback the transaction
func (tx *Tx) Rollback() error {
	return tx.Ptx.Rollback()
}

// Commit the transaction
func (tx *Tx) Commit() error {
	return tx.Ptx.Commit()
}

// Query runs the sql indentified by name. Return a row set.
func (tx *Tx) Query(name string, args ...interface{}) (*pgx.Rows, error) {
	sql, err := tx.qm.getQuery(name)
	if err != nil {
		return nil, err
	}
	return tx.Ptx.Query(sql, args...)
}

// QueryRow runs the sql identified by name. It returns a single row.
// Not that an error is only returned if the query is not defined.
// A query error is defered untill row.Scan is run. See pgx docs for more info.
func (tx *Tx) QueryRow(name string, args ...interface{}) (*pgx.Row, error) {
	sql, err := tx.qm.getQuery(name)
	if err != nil {
		return nil, err
	}
	return tx.Ptx.QueryRow(sql, args...), nil
}

// Exec runs the sql identified by name. Returns the result of the exec or an error.
func (tx *Tx) Exec(name string, args ...interface{}) (pgx.CommandTag, error) {
	sql, err := tx.qm.getQuery(name)
	if err != nil {
		return "", err
	}
	return tx.Ptx.Exec(sql, args...)
}
