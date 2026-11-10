package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// store is the Catalog's SQLite file (implementation §1.2.11): the entries, the
// topics of the Topic Registry and the bundles, the qualified records with
// their revision, and the hashes of the credentials.
type store struct {
	db *sql.DB
	// now is the clock records expire against.
	now func() time.Time
}

const schema = `
CREATE TABLE IF NOT EXISTS entries (
	digest          TEXT PRIMARY KEY,
	half            BLOB NOT NULL,
	half_digest     TEXT NOT NULL,
	nri_version     TEXT NOT NULL,
	artifact_size   INTEGER NOT NULL,
	verdict         TEXT NOT NULL,
	ruleset_version TEXT NOT NULL,
	findings        TEXT NOT NULL,
	scanned_at      INTEGER NOT NULL,
	deletion_mark   INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS topics (
	topic       TEXT PRIMARY KEY,
	description TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS bundles (
	bundle_id    TEXT PRIMARY KEY,
	purpose_text TEXT NOT NULL,
	members      TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS records (
	network    TEXT NOT NULL,
	key        TEXT NOT NULL,
	value      BLOB NOT NULL,
	revision   INTEGER NOT NULL,
	expires_at INTEGER,
	PRIMARY KEY (network, key)
);
CREATE INDEX IF NOT EXISTS records_key ON records (key);
CREATE TABLE IF NOT EXISTS credentials (
	hash    TEXT PRIMARY KEY,
	network TEXT NOT NULL,
	scope   TEXT NOT NULL
);
`

// openStore opens the file at path, creating its tables. One connection
// serializes the writes, which keeps each conditional write atomic.
func openStore(path string) (*store, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("catalog: schema: %w", err)
	}
	return &store{db: db, now: time.Now}, nil
}

func (s *store) close() error { return s.db.Close() }

// stamp is an as_of as the file keeps it, to the millisecond, so an entry reads
// back the same as it was answered.
func (s *store) stamp() time.Time { return s.now().UTC().Truncate(time.Millisecond) }

// entry is a stored entry: the half the act of registration carried, without
// the acquisition row, and what the Catalog adds to it.
type entry struct {
	Digest string
	// Half is the registered half as a JSON object, unknown keys included, and
	// HalfDigest its digest, which tells a second registration of the same half
	// from another one.
	Half         []byte
	HalfDigest   string
	NRIVersion   string
	ArtifactSize int64
	Scan         ScanResult
	DeletionMark bool
}

// view is the entry as the reads return it: the half with the scan_result, the
// nri_version, the artifact_size in bytes and the deletion_mark.
func (e *entry) view() (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(e.Half, &fields); err != nil {
		return nil, err
	}
	for k, v := range map[string]any{
		"scan_result":   e.Scan,
		"nri_version":   e.NRIVersion,
		"artifact_size": e.ArtifactSize,
		"deletion_mark": e.DeletionMark,
	} {
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		fields[k] = b
	}
	return json.Marshal(fields)
}

const entryColumns = `digest, half, half_digest, nri_version, artifact_size, verdict, ruleset_version, findings, scanned_at, deletion_mark`

func scanEntry(row interface{ Scan(...any) error }) (*entry, error) {
	var e entry
	var findings string
	var scanned int64
	if err := row.Scan(&e.Digest, &e.Half, &e.HalfDigest, &e.NRIVersion, &e.ArtifactSize,
		&e.Scan.Verdict, &e.Scan.RulesetVersion, &findings, &scanned, &e.DeletionMark); err != nil {
		return nil, err
	}
	if findings != "" {
		e.Scan.Findings = strings.Split(findings, ",")
	}
	e.Scan.AsOf = time.UnixMilli(scanned).UTC()
	return &e, nil
}

// entry returns the entry of digest, or nil.
func (s *store) entry(ctx context.Context, digest string) (*entry, error) {
	e, err := scanEntry(s.db.QueryRowContext(ctx, `SELECT `+entryColumns+` FROM entries WHERE digest = ?`, digest))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return e, err
}

func (s *store) entries(ctx context.Context) ([]*entry, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+entryColumns+` FROM entries ORDER BY digest`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*entry
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// insertEntry stores a new entry and, in the same transaction, the topic it
// registers when t is not nil.
func (s *store) insertEntry(ctx context.Context, e *entry, t *Topic) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }() // after Commit it does nothing
	if _, err := tx.ExecContext(ctx, `INSERT INTO entries (`+entryColumns+`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.Digest, e.Half, e.HalfDigest, e.NRIVersion, e.ArtifactSize,
		e.Scan.Verdict, e.Scan.RulesetVersion, strings.Join(e.Scan.Findings, ","), e.Scan.AsOf.UnixMilli(), e.DeletionMark); err != nil {
		return err
	}
	if t != nil {
		if err := insertTopic(ctx, tx, t); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// insertTopic registers a topic the Topic Registry does not have; a registered
// one keeps its description (architecture §3.7.6).
func insertTopic(ctx context.Context, x interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, t *Topic) error {
	_, err := x.ExecContext(ctx, `INSERT OR IGNORE INTO topics (topic, description) VALUES (?, ?)`, t.Topic, t.Description)
	return err
}

// putTopic registers a topic outside an entry's insertion.
func (s *store) putTopic(ctx context.Context, t *Topic) error { return insertTopic(ctx, s.db, t) }

// topic returns the Topic Registry's row of name, or nil.
func (s *store) topic(ctx context.Context, name string) (*Topic, error) {
	t := Topic{Topic: name}
	err := s.db.QueryRowContext(ctx, `SELECT description FROM topics WHERE topic = ?`, name).Scan(&t.Description)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (s *store) topics(ctx context.Context) ([]Topic, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT topic, description FROM topics ORDER BY topic`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Topic{}
	for rows.Next() {
		var t Topic
		if err := rows.Scan(&t.Topic, &t.Description); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// setDeletionMark stamps or removes the deletion mark, and says whether the
// entry exists.
func (s *store) setDeletionMark(ctx context.Context, digest string, mark bool) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE entries SET deletion_mark = ? WHERE digest = ?`, mark, digest)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (s *store) setScan(ctx context.Context, digest string, r ScanResult) error {
	_, err := s.db.ExecContext(ctx, `UPDATE entries SET verdict = ?, ruleset_version = ?, findings = ?, scanned_at = ? WHERE digest = ?`,
		r.Verdict, r.RulesetVersion, strings.Join(r.Findings, ","), r.AsOf.UnixMilli(), digest)
	return err
}

// occupied is the sum of the artifact_size of the entries without a deletion
// mark (architecture §3.13.3).
func (s *store) occupied(ctx context.Context) (int64, error) {
	var n sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT SUM(artifact_size) FROM entries WHERE deletion_mark = 0`).Scan(&n)
	return n.Int64, err
}

// stale are the digests of the entries without a deletion mark analyzed with a
// ruleset other than current, those with a current presence mark first
// (implementation §1.2.19).
func (s *store) stale(ctx context.Context, current string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT e.digest FROM entries e
		WHERE e.ruleset_version != ? AND e.deletion_mark = 0
		ORDER BY EXISTS (
			SELECT 1 FROM records r
			WHERE r.key = 'presence:' || e.digest AND (r.expires_at IS NULL OR r.expires_at > ?)
		) DESC, e.digest`, current, s.now().UnixMilli())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *store) bundle(ctx context.Context, id string) (*Bundle, error) {
	var b Bundle
	var members string
	err := s.db.QueryRowContext(ctx, `SELECT bundle_id, purpose_text, members FROM bundles WHERE bundle_id = ?`, id).
		Scan(&b.BundleID, &b.PurposeText, &members)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	b.Members = strings.Split(members, ",")
	return &b, nil
}

func (s *store) bundles(ctx context.Context) ([]Bundle, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT bundle_id, purpose_text, members FROM bundles ORDER BY bundle_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Bundle{}
	for rows.Next() {
		var b Bundle
		var members string
		if err := rows.Scan(&b.BundleID, &b.PurposeText, &members); err != nil {
			return nil, err
		}
		b.Members = strings.Split(members, ",")
		out = append(out, b)
	}
	return out, rows.Err()
}

// putBundle stores a bundle; the same content is the same bundle_id, so a
// second one changes nothing.
func (s *store) putBundle(ctx context.Context, b *Bundle) error {
	_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO bundles (bundle_id, purpose_text, members) VALUES (?, ?, ?)`,
		b.BundleID, b.PurposeText, strings.Join(b.Members, ","))
	return err
}

// errPrecondition is a conditional write whose condition does not hold.
var errPrecondition = errors.New("catalog: precondition failed")

// condition is what a conditional write requires: that the key not exist, or
// that its revision be one; the zero value requires nothing.
type condition struct {
	absent   bool
	revision int64
}

// record returns the current record of network and key: an expired one is
// absent for every read (implementation §1.2.11).
func (s *store) record(ctx context.Context, network, key string) (*Record, error) {
	r := Record{Network: network, Key: key}
	err := s.db.QueryRowContext(ctx, `SELECT value, revision FROM records WHERE network = ? AND key = ? AND (expires_at IS NULL OR expires_at > ?)`,
		network, key, s.now().UnixMilli()).Scan(&r.Value, &r.Revision)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// records lists the current records whose key starts with prefix, of network
// or, with it empty, of all networks.
func (s *store) records(ctx context.Context, prefix, network string) ([]Record, error) {
	q := `SELECT network, key, value, revision FROM records
		WHERE substr(key, 1, length(?)) = ? AND (expires_at IS NULL OR expires_at > ?)`
	args := []any{prefix, prefix, s.now().UnixMilli()}
	if network != "" {
		q += ` AND network = ?`
		args = append(args, network)
	}
	rows, err := s.db.QueryContext(ctx, q+` ORDER BY network, key`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Record{}
	for rows.Next() {
		var r Record
		if err := rows.Scan(&r.Network, &r.Key, &r.Value, &r.Revision); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// putRecord writes a record under cond, with a TTL when ttl is above zero, and
// returns its new revision. An expired record counts as absent.
func (s *store) putRecord(ctx context.Context, network, key string, value []byte, cond condition, ttl time.Duration) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }() // after Commit it does nothing
	now := s.now().UnixMilli()
	var revision int64
	var expires sql.NullInt64
	err = tx.QueryRowContext(ctx, `SELECT revision, expires_at FROM records WHERE network = ? AND key = ?`, network, key).Scan(&revision, &expires)
	exists := err == nil && (!expires.Valid || expires.Int64 > now)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	switch {
	case cond.absent && exists:
		return 0, errPrecondition
	case cond.revision != 0 && (!exists || revision != cond.revision):
		return 0, errPrecondition
	}
	// The revision keeps growing across an expiry, so an If-Match read before
	// it never matches what was written after.
	revision++
	var expiresAt any
	if ttl > 0 {
		expiresAt = now + ttl.Milliseconds()
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO records (network, key, value, revision, expires_at) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (network, key) DO UPDATE SET value = excluded.value, revision = excluded.revision, expires_at = excluded.expires_at`,
		network, key, value, revision, expiresAt); err != nil {
		return 0, err
	}
	return revision, tx.Commit()
}

// deleteRecord deletes a record, under the revision when it is not zero.
func (s *store) deleteRecord(ctx context.Context, network, key string, revision int64) error {
	if revision == 0 {
		_, err := s.db.ExecContext(ctx, `DELETE FROM records WHERE network = ? AND key = ?`, network, key)
		return err
	}
	res, err := s.db.ExecContext(ctx, `DELETE FROM records WHERE network = ? AND key = ? AND revision = ? AND (expires_at IS NULL OR expires_at > ?)`,
		network, key, revision, s.now().UnixMilli())
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return errPrecondition
	}
	return nil
}

func (s *store) addCredential(ctx context.Context, hash, network, scope string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO credentials (hash, network, scope) VALUES (?, ?, ?)`, hash, network, scope)
	return err
}

// credential returns the network and scope of the credential with hash, or
// false.
func (s *store) credential(ctx context.Context, hash string) (network, scope string, ok bool, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT network, scope FROM credentials WHERE hash = ?`, hash).Scan(&network, &scope)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", false, nil
	}
	return network, scope, err == nil, err
}

// revoke deletes every credential of network.
func (s *store) revoke(ctx context.Context, network string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM credentials WHERE network = ?`, network)
	return err
}
