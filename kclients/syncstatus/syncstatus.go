package syncstatus

import (
	"context"
	"encoding/json"
	"github.com/ava-labs/coreth/kclients/util/env"
	"github.com/ava-labs/coreth/kclients/util/sig"
	"github.com/ethereum/go-ethereum/log"
	"github.com/go-redis/redis/v8"
)

type Result struct {
	BlockNumber int64 `json:"blockNumber"`
}

var rdb *redis.Client

type redisHeight struct {
	testRedisHeight int64
	testOffset      int64
}

var rh redisHeight

var (
	addrs      = []string{}
	masterName string
	password   string
	db         int

	chainName string
)

func Start() {
	if rdb != nil {
		return
	}
	addrs = env.LoadEnvStrings(env.EnvETLAddrs)
	if len(addrs) == 0 {
		sig.Int("empty etl redis endpoint list")
		return
	}
	masterName = env.LoadEnvString(env.EnvETLMasterName)
	if masterName == "" {
		sig.Int("invalid etl redis master name")
		return
	}
	password = env.LoadEnvStringMute(env.EnvETLPassword)
	if password == "" {
		sig.Int("invalid etl redis password")
		return
	}
	chainName = env.LoadEnvString(env.EnvETLChainName)
	if chainName == "" {
		sig.Int("invalid etl chain name")
		return
	}
	db = env.LoadEnvInt(env.EnvETLDB)
	if db == env.WrongInt {
		return
	}

	rh.testOffset = env.LoadEnvInt64(env.EnvTestOffset)
	if rh.testOffset == env.WrongInt {
		return
	}
	rh.testRedisHeight = env.LoadEnvInt64(env.EnvTestRedisHeight)
	if rh.testRedisHeight == env.WrongInt {
		return
	}
}

func RedisHeight() int64 {
	if rh.testRedisHeight != 0 {
		return rh.testRedisHeight
	}

	if rdb == nil {
		rdb = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    masterName,
			SentinelAddrs: addrs,
			Password:      password,
			DB:            db,
		})
	}
	ctx := context.Background()
	str, err := rdb.HGet(ctx, "chain_latest:timeline", chainName).Result()
	if err != nil {
		log.Error("### DEBUG ### redis HGet err", "err", err)
		return -1
	}
	var r Result
	err = json.Unmarshal([]byte(str), &r)
	if err != nil {
		log.Error("### DEBUG ### json unmarshall err", "err", err)
		return -1
	}
	return r.BlockNumber - rh.testOffset
}
