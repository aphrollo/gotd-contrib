package postgresql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/go-faster/errors"
	"github.com/gotd/contrib/auth/kv"
	"github.com/gotd/td/session"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Ensure interface compliance.
var _ session.Storage = SessionStorage{}
var _ kv.Storage = &Client{}

// SessionStorage is a MTProto session PostgreSQL storage.
type SessionStorage struct {
	kv.Session
}

// NewSessionStorage creates new SessionStorage for a given id and table.
func NewSessionStorage(pool *pgxpool.Pool, id int64, table string) SessionStorage {
	client := &Client{
		pool:  pool,
		id:    id,
		table: table,
	}
	return SessionStorage{
		Session: kv.NewSession(client, fmt.Sprintf("%d", id)),
	}
}

// Client implements kv.Storage on top of PostgreSQL single table.
type Client struct {
	pool  *pgxpool.Pool
	id    int64
	table string // e.g. "hive.agents"
}

// Set updates only the session_string field in the JSONB data column.
func (c *Client) Set(ctx context.Context, _ string, sessionStr string) error {
	query := fmt.Sprintf(`
		UPDATE %s
		SET data = jsonb_set(coalesce(data, '{}'), '{session_string}', to_jsonb($1::text), true),
		    updated_at = NOW()
		WHERE id = $2
	`, c.table)

	_, err := c.pool.Exec(ctx, query, sessionStr, c.id)
	if err != nil {
		return fmt.Errorf("postgres: failed to update session_string for agent_id=%d: %w", c.id, err)
	}
	return nil
}

// Get retrieves the full JSONB data for the agent.
func (c *Client) Get(ctx context.Context, _ string) (string, error) {
	query := fmt.Sprintf(`
		SELECT data
		FROM %s
		WHERE id = $1
	`, c.table)

	var data sql.NullString
	err := c.pool.QueryRow(ctx, query, c.id).Scan(&data)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", kv.ErrKeyNotFound
		}
		return "", fmt.Errorf("postgres: failed to get session for agent_id=%d: %w", c.id, err)
	}
	if !data.Valid {
		return "", kv.ErrKeyNotFound
	}

	// Validate JSON
	var tmp json.RawMessage
	if err := json.Unmarshal([]byte(data.String), &tmp); err != nil {
		return "", fmt.Errorf("postgres: invalid JSON data for agent_id=%d: %w", c.id, err)
	}

	return data.String, nil
}
