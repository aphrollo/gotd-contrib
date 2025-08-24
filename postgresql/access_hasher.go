package postgresql

import (
	"context"
	"fmt"
	"time"

	"github.com/gotd/td/telegram/updates"
	"github.com/jackc/pgx/v5/pgxpool"
)

var _ updates.ChannelAccessHasher = (*AccessHasher)(nil)

// AccessHasher is a PostgreSQL-backed channel access hasher.
type AccessHasher struct {
	pool  *pgxpool.Pool
	table string // e.g. "hive.channel_accesshash"
}

// NewAccessHasher creates a new AccessHasher for the given pool and table.
func NewAccessHasher(pool *pgxpool.Pool, table string) *AccessHasher {
	return &AccessHasher{
		pool:  pool,
		table: table,
	}
}

// GetChannelAccessHash retrieves the access hash for a given user and channel.
func (h *AccessHasher) GetChannelAccessHash(ctx context.Context, userID, channelID int64) (int64, bool, error) {
	query := fmt.Sprintf(`
		SELECT hash
		FROM %s
		WHERE user_id = $1 AND channel_id = $2
	`, h.table)

	var accessHash int64
	err := h.pool.QueryRow(ctx, query, userID, channelID).Scan(&accessHash)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("postgres: failed to query channel access hash: %w", err)
	}

	return accessHash, true, nil
}

// SetChannelAccessHash inserts or updates the access hash for a user and channel.
func (h *AccessHasher) SetChannelAccessHash(ctx context.Context, userID, channelID, accessHash int64) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (user_id, channel_id, hash, updated)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (user_id, channel_id)
		DO UPDATE SET
			hash = EXCLUDED.hash,
			updated = CASE WHEN %s.hash != EXCLUDED.hash THEN EXCLUDED.updated ELSE %s.updated END
	`, h.table, h.table, h.table)

	_, err := h.pool.Exec(ctx, query, userID, channelID, accessHash, time.Now())
	if err != nil {
		return fmt.Errorf("postgres: failed to set channel access hash: %w", err)
	}

	return nil
}
