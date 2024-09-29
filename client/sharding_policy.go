package client

import (
	"math/rand"
	"time"
	log "github.com/scalog/scalog/logger"

	"github.com/scalog/scalog/pkg/view"
)

type DefaultShardingPolicy struct {
	shardID    int32
	replicaID  int32
	numReplica int32
	seed       rand.Source
}

func NewDefaultShardingPolicy(numReplica int32) *DefaultShardingPolicy {
	s := &DefaultShardingPolicy{
		shardID:    -1,
		replicaID:  -1,
		numReplica: numReplica,
		seed:       rand.NewSource(time.Now().UnixNano()),
	}
	return s
}

func (p *DefaultShardingPolicy) Shard(view *view.View, record string) (int32, int32) {
	if view == nil {
		return -1, -1
	}
	s, err := view.Get(p.shardID)
	if err == nil && s {
		return p.shardID, p.replicaID
	}
	numLiveShards := len(view.LiveShards)
	if numLiveShards < 1 {
		return -1, -1
	}
	rs := rand.New(p.seed).Intn(numLiveShards)
	rr := int32(rand.New(p.seed).Intn(int(p.numReplica)))
	p.shardID = view.LiveShards[rs]
	p.replicaID = rr
	return p.shardID, p.replicaID
}

func (p *DefaultShardingPolicy) AssignSpecificShard(view *view.View, record string, appenderId int32) (int32, int32) {
	if view == nil {
		return -1, -1
	}
	s, err := view.Get(p.shardID)
	if err == nil && s {
		return p.shardID, p.replicaID
	}
	numLiveShards := len(view.LiveShards)
	if numLiveShards < 1 {
		return -1, -1
	}

	if appenderId >= (int32(numLiveShards) * p.numReplica) {
		log.Errorf("appenderId %v is out of range", appenderId)
		return -1, -1
	}

	rs := appenderId / p.numReplica
	rr := appenderId % p.numReplica
	p.shardID = view.LiveShards[rs]
	p.replicaID = rr
	return p.shardID, p.replicaID
}