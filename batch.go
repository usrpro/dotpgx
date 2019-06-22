package dotpgx

import (
	"context"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

// Batch represents a pgx btach and the loaded query map.
// Support is still primitive and limited for our own use in migrations.
type Batch struct {
	// Pgx provides direct access to the pgx batch object
	Pgx *pgx.Batch
	qm  queryMap
}

// BeginBatch starts a new pgx batch.
func (db *DB) BeginBatch() *Batch {
	return &Batch{
		Pgx: db.Pool.BeginBatch(),
		qm:  db.qm,
	}
}

// BeginBatch starts a new pgx batch inside the current transaction
func (tx *Tx) BeginBatch() *Batch {
	return &Batch{
		Pgx: tx.Ptx.BeginBatch(),
		qm:  tx.qm,
	}
}

// Queue a query by name
func (b *Batch) Queue(name string, arguments []interface{}, parameterOIDs []pgtype.OID, resultFormatCodes []int16) (err error) {
	q, err := b.qm.getQuery(name)
	if err != nil {
		return
	}
	b.Pgx.Queue(q.getSQL(), arguments, parameterOIDs, resultFormatCodes)
	return
}

// QueueAll the registered queries, sorted by name.
func (b *Batch) QueueAll() {
	mutex.Lock()
	index := b.qm.sort()
	for _, v := range index {
		b.Queue(v, nil, nil, nil)
	}
	mutex.Unlock()
}

// Close the batch operation
func (b *Batch) Close() error {
	return b.Pgx.Close()
}

// Send the batch
func (b *Batch) Send() error {
	return b.Pgx.Send(context.TODO(), nil)
}

// ExecResults reads the results from the next query in the batch as if the query has been sent with Exec.
func (b *Batch) ExecResults() (pgx.CommandTag, error) {
	return b.Pgx.ExecResults()
}

// QueryResults reads the results from the next query in the batch as if the query has been sent with Query.
func (b *Batch) QueryResults() (*pgx.Rows, error) {
	return b.Pgx.QueryResults()
}

// QueryRowResults reads the results from the next query in the batch as if the query has been sent with QueryRow.
func (b *Batch) QueryRowResults() *pgx.Row {
	return b.Pgx.QueryRowResults()
}
