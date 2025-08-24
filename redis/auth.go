package redis

import (
	"github.com/redis/go-redis/v9"

	"github.com/gotd/contrib/auth/kv"
)

// Credentials stores user credentials to Redis.
type Credentials struct {
	kv.Credentials
}

// NewCredentials creates new Credentials.
func NewCredentials(client *redis.Client) Credentials {
	s := redisClient{
		client: client,
	}
	return Credentials{kv.NewCredentials(s)}
}
