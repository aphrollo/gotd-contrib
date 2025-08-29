package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/go-faster/errors"
	"github.com/gotd/td/telegram/updates"
	"github.com/redis/go-redis/v9"
)

var _ updates.ChannelAccessHasher = (*AccessHasher)(nil)

// AccessHasher stores channel access hashes in Redis
type AccessHasher struct {
	client *redis.Client
	prefix string // e.g., "channel_accesshash:"
	ttl    time.Duration
}

// NewAccessHasher creates a new Redis-backed access hasher.
// ttl can be 0 for permanent storage.
func NewAccessHasher(client *redis.Client, prefix string, ttl time.Duration) *AccessHasher {
	return &AccessHasher{
		client: client,
		prefix: prefix,
		ttl:    ttl,
	}
}

// key generates a Redis key for user+channel
func (h *AccessHasher) key(userID, channelID int64) string {
	return fmt.Sprintf("%s%d:%d", h.prefix, userID, channelID)
}

// GetChannelAccessHash retrieves the access hash for a given user and channel
func (h *AccessHasher) GetChannelAccessHash(ctx context.Context, userID, channelID int64) (int64, bool, error) {
	key := h.key(userID, channelID)
	val, err := h.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("redis: failed to get channel access hash: %w", err)
	}

	var hash int64
	if _, err := fmt.Sscanf(val, "%d", &hash); err != nil {
		return 0, false, fmt.Errorf("redis: failed to parse access hash: %w", err)
	}
	return hash, true, nil
}

// SetChannelAccessHash stores or updates the access hash
func (h *AccessHasher) SetChannelAccessHash(ctx context.Context, userID, channelID, accessHash int64) error {
	key := h.key(userID, channelID)
	val := fmt.Sprintf("%d", accessHash)
	var err error
	if h.ttl > 0 {
		err = h.client.Set(ctx, key, val, h.ttl).Err()
	} else {
		err = h.client.Set(ctx, key, val, 0).Err()
	}
	if err != nil {
		return fmt.Errorf("redis: failed to set channel access hash: %w", err)
	}
	return nil
}
