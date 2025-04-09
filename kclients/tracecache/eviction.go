package tracecache

import (
	"context"
	"fmt"
	"strconv"

	"github.com/ethereum/go-ethereum/log"
)

// Sweep deletes the already consumed data in redis at the beginning of starting live trace service
func (c *redisCache) Sweep(syncedHeight int64) {
	all, err := c.rdb.HKeys(context.Background(), c.key).Result()
	if err != nil {
		log.Error("### DEBUG ### [redisCache.Clean] failed to get all keys", "err", err)
		return
	}
	for _, k := range all {
		height, err := strconv.Atoi(k)
		if err != nil {
			log.Error("### DEBUG ### [redisCache.Clean] invalid key", "key", fmt.Sprintf("%s:%v", c.key, k))
		}
		if int64(height) <= syncedHeight {
			n, err := c.rdb.HDel(context.Background(), c.key, k).Result()
			if err != nil {
				log.Error("### DEBUG ### [redisCache.Clean] failed to del key", "key", fmt.Sprintf("%s:%v", c.key, k))
			} else if n > 0 {
				log.Info("### DEBUG ### [redisCache.Clean] sweep consumed data", "key", fmt.Sprintf("%s:%v", c.key, k), "n", n)
			}
		}
	}
	c.cut = syncedHeight
}

// Cut deletes the data consumed recently
func (c *redisCache) Cut(to int64) {
	if c.cut <= 0 {
		log.Warn("### DEBUG ### [redisCache.Cut] no last cut point", "key", c.key)
		return
	}
	if to == c.cut {
		return
	}
	from := c.cut + 1
	cutCnt := 0
	for i := from; i <= to; i++ {
		n, err := c.rdb.HDel(context.Background(), c.key, fmt.Sprint(i)).Result()
		if err != nil {
			log.Error("### DEBUG ### [redisCache.Cut]", "key", fmt.Sprintf("%s:%v", c.key, i), "err", err)
		} else {
			cutCnt += int(n)
		}
	}
	c.cut = to
	log.Info("### DEBUG ### [redisCache.Cut]", "[from,to]", fmt.Sprintf("[%d,%d]", from, to), "progress", fmt.Sprintf("%d/%d", cutCnt, to-from+1))
}
