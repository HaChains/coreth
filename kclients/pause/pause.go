package pause

import (
	"context"
	"fmt"
	"github.com/ava-labs/coreth/kclients/syncstatus"
	"github.com/ava-labs/coreth/kclients/util/env"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
)

var pc pauseControl

type pauseControl struct {
	ctx    context.Context
	cancel context.CancelFunc

	enabled     bool
	started     bool
	allowOffset int64
	//l2Height    int64
	nextHeight  int64
	redisHeight int64
	lock        sync.RWMutex
}

func Start() {
	pc.enabled = env.LoadEnvBool(env.EnvETLPauseEnabled)
	if !pc.enabled {
		log.Info("### DEBUG ### pause control service is not enabled")
		return
	}
	pc.allowOffset = env.LoadEnvInt64(env.EnvETLAllowBehind)
	if pc.allowOffset == env.WrongInt {
		return
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	pc.ctx = ctx
	pc.cancel = cancelFunc

	go pc.updateLoop()
	log.Info("### DEBUG ### pause control service started")
	pc.started = true
}

func Stop() {
	if !Started() {
		return
	}
	log.Info("### DEBUG ### stopping pause control service")
	pc.cancel()
}

func (c *pauseControl) updateLoop() {
	c.updateBlockHeight()
	for {
		select {
		case <-time.After(time.Second):
			c.updateBlockHeight()
		case <-c.ctx.Done():
			log.Info("### DEBUG ### pauseControl updateLoop stopped")
			return
		}
	}
}

func (c *pauseControl) updateBlockHeight() {
	redisHeight := syncstatus.RedisHeight()

	c.lock.Lock()
	if redisHeight != -1 {
		c.redisHeight = redisHeight
	}
	c.lock.Unlock()
}

func Started() bool {
	return pc.started
}

func RedisBehind(nextHeight int64) bool {
	if pc.started == false {
		return false
	}
	return pc.redisBehind(nextHeight)
}

func PauseIfBehind(tag string) (shutdown bool) {
	return pc.pauseIfBehind(tag)
}
func (c *pauseControl) redisBehind(nextHeight int64) bool {
	if !Started() {
		fmt.Println("### DEBUG ### [pauseControl.redisBehind] service is not started yet")
		Start()
		time.Sleep(5 * time.Second)
	}
	c.lock.RLock()
	if nextHeight == 0 {
		nextHeight = c.nextHeight
	} else {
		c.nextHeight = nextHeight
	}
	var pause = nextHeight-c.redisHeight >= c.allowOffset
	c.lock.RUnlock()
	log.Info(fmt.Sprintf("### DEBUG ### nextHeight(%d)-redisHeight(%d) = %d >= allowOffset(%d): %v",
		nextHeight, c.redisHeight, nextHeight-c.redisHeight, c.allowOffset, pause))
	return pause
}

func (c *pauseControl) pauseIfBehind(tag string) (shutdown bool) {
	stopCh := make(chan struct{})
	for {
		select {
		case <-time.After(1 * time.Second):
			//log.Debug(fmt.Sprintf("### DEBUG ### check redis block height [%s]", tag))
			if !c.redisBehind(0) {
				close(stopCh)
			}
		case <-stopCh:
			log.Info("### DEBUG ### stop pause", "tag", tag)
			return false
		case <-c.ctx.Done():
			log.Info("### DEBUG ### Pause Control exit", "tag", tag)
			return true
		}
	}
}
