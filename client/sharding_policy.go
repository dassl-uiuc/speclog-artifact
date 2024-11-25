package client

import (
	"log"
	"math/rand"
	"sort"
	"time"
	log "github.com/scalog/scalog/logger"

	"github.com/scalog/scalog/pkg/view"
)

type ShardingPolicyWithHint struct {
	shardID      int32
	replicaID    int32
	numReplica   int32
	shardingHint int64
}

func NewShardingPolicyWithHint(numReplica int32, shardingHint int64) *ShardingPolicyWithHint {
	s := &ShardingPolicyWithHint{
		shardID:      -1,
		replicaID:    -1,
		numReplica:   numReplica,
		shardingHint: shardingHint,
	}
	return s
}

func (p *ShardingPolicyWithHint) GetShardID() int32 {
	return p.shardID
}

func (p *ShardingPolicyWithHint) GetReplicaID() int32 {
	return p.replicaID
}

func (p *ShardingPolicyWithHint) Shard(view *view.View, record string) (int32, int32) {
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

	// sort list of live shards to ensure uniform order across clients
	sort.Slice(view.LiveShards, func(i, j int) bool {
		return view.LiveShards[i] < view.LiveShards[j]
	})

	replicaId := p.shardingHint % (int64(p.numReplica) * int64(numLiveShards))
	rs := int32(replicaId / int64(p.numReplica))
	rr := int32(replicaId % int64(p.numReplica))
	p.shardID = view.LiveShards[rs]
	p.replicaID = rr
	log.Printf("sharding hint: %v, shardID: %v, replicaID: %v", p.shardingHint, p.shardID, p.replicaID)
	return p.shardID, p.replicaID
}

func (p *ShardingPolicyWithHint) AssignSpecificShard(view *view.View, record string, appenderId int32) (int32, int32) {
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
		return -1, -1
	}

	// sort list of live shards to ensure uniform order across clients
	sort.Slice(view.LiveShards, func(i, j int) bool {
		return view.LiveShards[i] < view.LiveShards[j]
	})

	rs := appenderId / p.numReplica
	rr := appenderId % p.numReplica
	p.shardID = view.LiveShards[rs]
	p.replicaID = rr
	log.Printf("appenderId: %v, shardID: %v, replicaID: %v", appenderId, p.shardID, p.replicaID)

	return p.shardID, p.replicaID
}

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

func (p *DefaultShardingPolicy) GetShardID() int32 {
	return p.shardID
}

func (p *DefaultShardingPolicy) GetReplicaID() int32 {
	return p.replicaID
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

	// sort list of live shards to ensure uniform order across clients
	sort.Slice(view.LiveShards, func(i, j int) bool {
		return view.LiveShards[i] < view.LiveShards[j]
	})

	rs := rand.New(p.seed).Intn(numLiveShards)
	rr := int32(rand.New(p.seed).Intn(int(p.numReplica)))
	p.shardID = view.LiveShards[rs]
	p.replicaID = rr
	log.Printf("shardID: %v, replicaID: %v", p.shardID, p.replicaID)

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
		return -1, -1
	}

	// sort list of live shards to ensure uniform order across clients
	sort.Slice(view.LiveShards, func(i, j int) bool {
		return view.LiveShards[i] < view.LiveShards[j]
	})

	rs := appenderId / p.numReplica
	rr := appenderId % p.numReplica
	p.shardID = view.LiveShards[rs]
	p.replicaID = rr

	log.Printf("appenderId: %v, shardID: %v, replicaID: %v", appenderId, p.shardID, p.replicaID)

	return p.shardID, p.replicaID
}