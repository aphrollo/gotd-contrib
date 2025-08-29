package redis

import (
	"context"
	"fmt"
	"strconv"
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

func (h *AccessHasher) key(userID int64) string {
	return fmt.Sprintf("%s%d", h.prefix, userID) // one key per agent
}

func (h *AccessHasher) GetChannelAccessHash(ctx context.Context, userID, channelID int64) (int64, bool, error) {
	key := h.key(userID)
	val, err := h.client.HGet(ctx, key, fmt.Sprintf("%d", channelID)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, false, nil
		}
		return 0, false, err
	}
	hash, _ := strconv.ParseInt(val, 10, 64)
	return hash, true, nil
}

func (h *AccessHasher) SetChannelAccessHash(ctx context.Context, userID, channelID, accessHash int64) error {
	key := h.key(userID)
	return h.client.HSet(ctx, key, fmt.Sprintf("%d", channelID), accessHash).Err()
}
