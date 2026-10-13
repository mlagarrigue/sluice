package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
)

// store is what the app asks of a driver, in the terms of the scenarios
// rather than of a wire protocol. Axis B of the study swaps the store and
// nothing else: the same gateway groups the unit reads, the same `= ANY($1)`
// fetches a batch in one round trip, the same pool of dbConns connections
// serves lists and writes. What -driver changes is the code between the app
// and the socket, which is the only thing the axis wants to measure.
//
// The unit read is one *batch* of keys, not one key: the gateway's pipeline
// calls it once per batch, from its one goroutine, and owns the map it fills,
// so a store may keep per-call scratch space without a lock.
type store interface {
	// unit fills found with the rows of ids, by id. A key the table does not
	// hold is simply absent; the caller turns that into errNotFound.
	unit(ctx context.Context, ids []int64, found map[int64]orderRow) error
	// list returns up to limit rows above from, in key order.
	list(ctx context.Context, from int64, limit int) ([]orderRow, error)
	// write inserts one row into comparison_writes.
	write(ctx context.Context, key, total int64) error
	// createWriteTable creates comparison_writes if it is missing and turns
	// autovacuum off on it.
	createWriteTable(ctx context.Context) error
	Close() error
}

func newStore(o options) (store, error) {
	switch o.driver {
	case driverSluice:
		return newSluiceStore(o)
	case driverPgx:
		return newPgxStore(o)
	}
	return nil, fmt.Errorf("-driver %q: unknown", o.driver)
}

// --- database/postgres: the reference configuration R ----------------------

// sluiceStore is R: one connection for the gateway, a channel of dbConns
// connections for lists and writes — the connector has no pool, and a
// channel is all a program with a fixed number of connections needs.
type sluiceStore struct {
	gwConn *postgres.Conn
	pool   chan *postgres.Conn
	size   int
	buf    []byte // the gateway's array parameter, reused between batches
}

func dial(cfg dbConfig) (*postgres.Conn, error) {
	nc, err := net.DialTimeout("tcp", cfg.addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	conn, err := postgres.Startup(nc, postgres.StartupConfig{
		User: cfg.user, Database: cfg.database, Password: cfg.password,
	})
	if err != nil {
		return nil, fmt.Errorf("postgres startup: %w", err)
	}
	return conn, nil
}

func newSluiceStore(o options) (*sluiceStore, error) {
	s := &sluiceStore{
		pool: make(chan *postgres.Conn, o.dbConns),
		size: o.gatewaySize,
		buf:  make([]byte, 0, 4+o.gatewaySize*12),
	}
	for range o.dbConns {
		c, err := dial(o.db)
		if err != nil {
			_ = s.Close()
			return nil, err
		}
		s.pool <- c
	}
	var err error
	if s.gwConn, err = dial(o.db); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func (s *sluiceStore) Close() error {
	var errs []error
	if s.gwConn != nil {
		errs = append(errs, s.gwConn.Close())
	}
	for len(s.pool) > 0 {
		errs = append(errs, (<-s.pool).Close())
	}
	return errors.Join(errs...)
}

func (s *sluiceStore) createWriteTable(ctx context.Context) error {
	c := <-s.pool
	defer func() { s.pool <- c }()
	if err := c.Exec(ctx, writeTable); err != nil {
		return err
	}
	return c.Exec(ctx, writeNoVacuum)
}

// unit is mostly the example of package gateway, with the label added: every
// key of the batch leaves as one array parameter and comes back in one round
// trip.
func (s *sluiceStore) unit(ctx context.Context, ids []int64, found map[int64]orderRow) error {
	s.buf = postgres.AppendInt8Array(s.buf[:0], ids)
	src := s.gwConn.Query(ctx, unitQuery, [][]byte{s.buf},
		postgres.QueryConfig{BatchRows: s.size + 1, ParamOIDs: []uint32{postgres.OIDInt8Array}})
	src.Stream()(func(rows sluice.Batch[postgres.Row]) bool {
		for _, r := range rows.Items {
			row, err := decodeOrder(r)
			if err != nil {
				continue
			}
			found[row.ID] = row
		}
		return true
	})
	return src.Err()
}

func (s *sluiceStore) list(ctx context.Context, from int64, limit int) ([]orderRow, error) {
	conn := <-s.pool
	defer func() { s.pool <- conn }()

	params := [][]byte{postgres.AppendInt8(nil, from), postgres.AppendInt8(nil, int64(limit))}
	items := make([]orderRow, 0, limit)
	src := conn.Query(ctx, listQuery, params, postgres.QueryConfig{
		BatchRows: 1024, ParamOIDs: []uint32{postgres.OIDInt8, postgres.OIDInt8},
	})
	for b := range src.Stream() {
		for _, r := range b.Items {
			row, err := decodeOrder(r)
			if err != nil {
				return nil, errDecode
			}
			items = append(items, row)
		}
	}
	return items, src.Err()
}

func (s *sluiceStore) write(ctx context.Context, key, total int64) error {
	conn := <-s.pool
	defer func() { s.pool <- conn }()

	params := [][]byte{postgres.AppendInt8(nil, key), postgres.AppendInt8(nil, total)}
	src := conn.Query(ctx, writeQuery, params, postgres.QueryConfig{
		BatchRows: 2, ParamOIDs: []uint32{postgres.OIDInt8, postgres.OIDInt8},
	})
	for range src.Stream() {
	}
	return src.Err()
}

func decodeOrder(r postgres.Row) (orderRow, error) {
	idB, _ := r.Value(0)
	totalB, _ := r.Value(1)
	labelB, _ := r.Value(2)
	id, err := postgres.DecodeInt8(idB)
	if err != nil {
		return orderRow{}, err
	}
	total, err := postgres.DecodeInt8(totalB)
	if err != nil {
		return orderRow{}, err
	}
	return orderRow{ID: id, Total: total, Label: postgres.DecodeText(labelB)}, nil
}

// --- pgx: axis B ------------------------------------------------------------

// pgxStore is axis B: pgx written the way its documentation writes it — a
// pgxpool of dbConns connections for lists and writes, the statement cache
// on — plus one *pgx.Conn* of its own for the gateway, as R gives the
// gateway a connection of its own. Same grouping, same SQL, same budget of
// connections: only the driver differs.
type pgxStore struct {
	gwConn *pgx.Conn
	pool   *pgxpool.Pool
}

func pgxURL(cfg dbConfig) string {
	// The connector dials in clear text (postgres.Startup over TCP), so pgx
	// must too, or axis B would be measuring TLS.
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(cfg.user, cfg.password),
		Host:     cfg.addr,
		Path:     "/" + cfg.database,
		RawQuery: "sslmode=disable",
	}
	return u.String()
}

func newPgxStore(o options) (*pgxStore, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg, err := pgxpool.ParseConfig(pgxURL(o.db))
	if err != nil {
		return nil, err
	}
	conns := int32(min(o.dbConns, maxDBConns)) //nolint:gosec // G115: bounded just above
	cfg.MaxConns = conns
	cfg.MinConns = conns
	s := &pgxStore{}
	if s.pool, err = pgxpool.NewWithConfig(ctx, cfg); err != nil {
		return nil, fmt.Errorf("pgx pool: %w", err)
	}
	if err := s.pool.Ping(ctx); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("pgx pool: %w", err)
	}
	if s.gwConn, err = pgx.ConnectConfig(ctx, cfg.ConnConfig); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("pgx connect: %w", err)
	}
	return s, nil
}

func (s *pgxStore) Close() error {
	var err error
	if s.gwConn != nil {
		err = s.gwConn.Close(context.Background())
	}
	if s.pool != nil {
		s.pool.Close()
	}
	return err
}

func (s *pgxStore) createWriteTable(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, writeTable); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, writeNoVacuum)
	return err
}

func (s *pgxStore) unit(ctx context.Context, ids []int64, found map[int64]orderRow) error {
	rows, err := s.gwConn.Query(ctx, unitQuery, ids)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var row orderRow
		if err := rows.Scan(&row.ID, &row.Total, &row.Label); err != nil {
			continue
		}
		found[row.ID] = row
	}
	return rows.Err()
}

func (s *pgxStore) list(ctx context.Context, from int64, limit int) ([]orderRow, error) {
	rows, err := s.pool.Query(ctx, listQuery, from, int64(limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]orderRow, 0, limit)
	for rows.Next() {
		var row orderRow
		if err := rows.Scan(&row.ID, &row.Total, &row.Label); err != nil {
			return nil, errDecode
		}
		items = append(items, row)
	}
	return items, rows.Err()
}

func (s *pgxStore) write(ctx context.Context, key, total int64) error {
	_, err := s.pool.Exec(ctx, writeQuery, key, total)
	return err
}
