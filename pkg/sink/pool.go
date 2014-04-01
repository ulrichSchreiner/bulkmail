package sink

import (
	"github.com/garyburd/redigo/redis"
	"time"
)

type Storer interface {
	GetConnection() redis.Conn
}

func newRedisPool(server string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

type redisStore struct {
	pool *redis.Pool
}

func (s *redisStore) GetConnection() redis.Conn {
	return s.pool.Get()
}

func NewStorer(server string) Storer {
	return &redisStore{newRedisPool(server)}
}
