package redis

import (
	"context"
	"fmt"
	"strconv"

	"github.com/go-faster/errors"
	"github.com/redis/go-redis/v9"

	"github.com/gotd/td/telegram/updates"
)

// StateStorage implements updates.StateStorage using Redis.
type StateStorage struct {
	client *redis.Client
}

// NewRedisStateStorage creates a new Redis-based state storage.
func NewRedisStateStorage(client *redis.Client) *StateStorage {
	return &StateStorage{client: client}
}

// helper for user key
func userKey(userID int64) string {
	return fmt.Sprintf("state:%d", userID)
}

// GetState loads the global state for a user.
func (s *StateStorage) GetState(ctx context.Context, userID int64) (updates.State, bool, error) {
	key := userKey(userID)
	fields := []string{"pts", "qts", "date", "seq"}

	vals, err := s.client.HMGet(ctx, key, fields...).Result()
	if err != nil {
		return updates.State{}, false, err
	}

	state := updates.State{}
	exists := false
	for i, v := range vals {
		if v == nil {
			continue
		}
		exists = true
		switch fields[i] {
		case "pts":
			state.Pts, _ = strconv.Atoi(v.(string))
		case "qts":
			state.Qts, _ = strconv.Atoi(v.(string))
		case "date":
			state.Date, _ = strconv.Atoi(v.(string))
		case "seq":
			state.Seq, _ = strconv.Atoi(v.(string))
		}
	}
	return state, exists, nil
}

// SetState sets all global state fields.
func (s *StateStorage) SetState(ctx context.Context, userID int64, state updates.State) error {
	key := userKey(userID)
	_, err := s.client.HSet(ctx, key, map[string]interface{}{
		"pts":  state.Pts,
		"qts":  state.Qts,
		"date": state.Date,
		"seq":  state.Seq,
	}).Result()
	return err
}

// helper: update a single field
func (s *StateStorage) setField(ctx context.Context, userID int64, field string, value int) error {
	key := userKey(userID)
	exists, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return err
	}
	if exists == 0 {
		return fmt.Errorf("user state does not exist")
	}
	_, err = s.client.HSet(ctx, key, field, value).Result()
	return err
}

// SetPts updates PTS
func (s *StateStorage) SetPts(ctx context.Context, userID int64, pts int) error {
	return s.setField(ctx, userID, "pts", pts)
}

// SetQts updates QTS
func (s *StateStorage) SetQts(ctx context.Context, userID int64, qts int) error {
	return s.setField(ctx, userID, "qts", qts)
}

// SetDate updates Date
func (s *StateStorage) SetDate(ctx context.Context, userID int64, date int) error {
	return s.setField(ctx, userID, "date", date)
}

// SetSeq updates Seq
func (s *StateStorage) SetSeq(ctx context.Context, userID int64, seq int) error {
	return s.setField(ctx, userID, "seq", seq)
}

// SetDateSeq updates both date and seq
func (s *StateStorage) SetDateSeq(ctx context.Context, userID int64, date, seq int) error {
	key := userKey(userID)
	exists, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return err
	}
	if exists == 0 {
		return fmt.Errorf("user state does not exist")
	}
	_, err = s.client.HMSet(ctx, key, map[string]interface{}{
		"date": date,
		"seq":  seq,
	}).Result()
	return err
}

// GetChannelPts retrieves the PTS for a specific channel
func (s *StateStorage) GetChannelPts(ctx context.Context, userID, channelID int64) (int, bool, error) {
	key := userKey(userID)
	field := fmt.Sprintf("channel:%d", channelID)
	v, err := s.client.HGet(ctx, key, field).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, false, nil
		}
		return 0, false, err
	}
	pts, _ := strconv.Atoi(v)
	return pts, true, nil
}

// SetChannelPts updates the PTS for a specific channel
func (s *StateStorage) SetChannelPts(ctx context.Context, userID, channelID int64, pts int) error {
	key := userKey(userID)
	field := fmt.Sprintf("channel:%d", channelID)
	_, err := s.client.HSet(ctx, key, field, pts).Result()
	return err
}

// ForEachChannels iterates over all channels for a user
func (s *StateStorage) ForEachChannels(ctx context.Context, userID int64, f func(ctx context.Context, channelID int64, pts int) error) error {
	key := userKey(userID)
	fields, err := s.client.HKeys(ctx, key).Result()
	if err != nil {
		return err
	}
	for _, field := range fields {
		if len(field) < 8 || field[:8] != "channel:" {
			continue
		}
		channelID, _ := strconv.ParseInt(field[8:], 10, 64)
		ptsStr, err := s.client.HGet(ctx, key, field).Result()
		if err != nil {
			return err
		}
		pts, _ := strconv.Atoi(ptsStr)
		if err := f(ctx, channelID, pts); err != nil {
			return err
		}
	}
	return nil
}
