package tracecache

import (
	"github.com/go-redis/redis/v8"
	"testing"
)

func newTestRedisCache() *redisCache {
	rdb := redis.NewClient(&redis.Options{
		Addr: "addr",
		DB:   0,
	})
	return &redisCache{
		key: "key",
		rdb: rdb,
	}
}

func TestSweep(t *testing.T) {
	c := newTestRedisCache()
	c.Sweep(10)
}

func TestCut(t *testing.T) {
	c := newTestRedisCache()
	to := int64(10)
	c.cut = to - 2
	c.Cut(to)
	c.Cut(to + 2)
	c.Cut(to + 4)
}
