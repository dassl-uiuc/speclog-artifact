package order

import (
	"maps"
	"math"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/order/orderpb"

	movingaverage "github.com/RobinUS2/golang-moving-average"
	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft/raftpb"
)

type OrderServer struct {
	index            int32
	numReplica       int32
	dataNumReplica   int32
	clientID         int32
	batchingInterval time.Duration
	isLeader         bool
	startGSN         int64
	viewID           int32          // use sync/atomic to access viewID
	shards           map[int32]bool // true for live shards, false for finalized ones
	forwardC         chan *orderpb.LocalCuts
	proposeC         chan *orderpb.CommittedEntry
	commitC          chan *orderpb.CommittedEntry
	finalizeC        chan *orderpb.FinalizeEntry
	subC             map[int32]chan *orderpb.CommittedEntry
	subCMu           sync.RWMutex
	prevCut          map[int32]int64

	rnConfChangeC      chan raftpb.ConfChange
	rnProposeC         chan string
	rnCommitC          <-chan *string
	rnErrorC           <-chan error
	rnSnapshotterReady <-chan *snap.Snapshotter

	lastCutTime          map[int32]time.Time
	avgDelta             map[int32]*movingaverage.MovingAverage
	quota                map[int32]int64
	prevLocalCutNum      map[int32]int64
	localCutChangeWindow int64
	quotaChanged         int64 // number of primaries for whom quota has been changed
}

// fraction of the local cut window at which quota change decisions are made for a shard
const quotaChangeFraction float64 = 0.75

// TODO: Currently assuming batching interval is the same as the ordering interval
// TODO: Currently assuming no change in number of shards
func NewOrderServer(index, numReplica, dataNumReplica int32, batchingInterval time.Duration, peerList []string) *OrderServer {
	s := &OrderServer{
		index:            index,
		numReplica:       numReplica,
		dataNumReplica:   dataNumReplica,
		viewID:           0,
		isLeader:         index == 0,
		batchingInterval: batchingInterval,
	}
	s.shards = make(map[int32]bool)
	s.forwardC = make(chan *orderpb.LocalCuts, 4096)
	s.proposeC = make(chan *orderpb.CommittedEntry, 4096)
	s.commitC = make(chan *orderpb.CommittedEntry, 4096)
	s.finalizeC = make(chan *orderpb.FinalizeEntry, 4096)
	s.subC = make(map[int32]chan *orderpb.CommittedEntry)

	s.lastCutTime = make(map[int32]time.Time)
	s.avgDelta = make(map[int32]*movingaverage.MovingAverage)
	s.quota = make(map[int32]int64)
	s.prevLocalCutNum = make(map[int32]int64)
	s.localCutChangeWindow = 10
	s.quotaChanged = 0

	s.rnConfChangeC = make(chan raftpb.ConfChange)
	s.rnProposeC = make(chan string)
	commitC, errorC, snapshotterReady := newRaftNode(
		int(index)+1, // raftNode is 1-indexed
		peerList,
		false, // not to join an existing cluster
		s.getSnapshot,
		s.rnProposeC,
		s.rnConfChangeC,
	)
	s.rnCommitC = commitC
	s.rnErrorC = errorC
	s.rnSnapshotterReady = snapshotterReady
	return s
}

func (s *OrderServer) Start() {
	go s.processReport()
	go s.runReplication()
	go s.processCommit()
	go s.processRNCommit()
}

// runReplication runs Raft to replicate proposed messages and receive
// committed messages.
func (s *OrderServer) runReplication() {
	for e := range s.proposeC {
		b, err := proto.Marshal(e)
		if err != nil {
			log.Errorf("%v", err)
			continue
		}
		s.rnProposeC <- string(b)
	}
}

func (s *OrderServer) computeCutDiff(pcut, ccut map[int32]int64) int64 {
	sum := int64(0)
	if pcut == nil {
		for _, v := range ccut {
			sum += v
		}
	} else {
		for k, v := range ccut {
			if vv, ok := pcut[k]; ok {
				sum += v - vv
			} else {
				sum += v
			}
		}
	}
	return sum

}

func (s *OrderServer) computeCommittedCut(lcs map[int32]*orderpb.LocalCut) map[int32]int64 {
	incrViewID := false
	// add new live shards
	for rid := range lcs {
		shard := rid / s.dataNumReplica
		if _, ok := s.shards[shard]; !ok {
			incrViewID = true
			s.shards[shard] = true
		}
	}
	ccut := make(map[int32]int64)
	for rid := range lcs {
		shard := rid / s.dataNumReplica
		status := s.shards[shard]
		// check if the shard is finialized
		if !status {
			incrViewID = true
			// clean finalized shards from lcs
			delete(lcs, rid)
			continue
		}
		localReplicaID := rid % s.dataNumReplica
		begin := rid - localReplicaID
		chosen := int64(0)
		for i := int32(0); i < s.dataNumReplica; i++ {
			if cut, ok := lcs[begin+i]; ok {
				if cut.Cut[localReplicaID] > 0 {
					chosen = cut.Cut[localReplicaID]
				}
			} else {
				chosen = int64(0)
			}
		}
		ccut[rid] = chosen
	}
	if incrViewID {
		atomic.AddInt32(&s.viewID, 1)
	}
	return ccut
}

// proposeCommit broadcasts entries in commitC to all subCs.
func (s *OrderServer) processReport() {
	lcsBatches := make(map[int64](map[int32]*orderpb.LocalCut)) // map from local cut number to the local cuts
	readyBatch := make(chan (int64), 2*s.localCutChangeWindow)  // channel on which ready batches of local cuts are sent
	lcs := make(map[int32]*orderpb.LocalCut)                    // all local cuts
	liveShardPrimaries := make([]int32, 0)
	for id, v := range s.shards {
		if v {
			for i := int32(0); i < s.dataNumReplica; i++ {
				liveShardPrimaries = append(liveShardPrimaries, id*s.dataNumReplica+i)
			}
		}
	}
	ticker := time.NewTicker(s.batchingInterval)
	for {
		select {
		case e := <-s.forwardC:
			// log.Debugf("processReport forwardC")
			if s.isLeader { // store local cuts
				for _, lc := range e.Cuts {
					id := lc.ShardID*s.dataNumReplica + lc.LocalReplicaID
					valid := true
					// check if the received cut is up-to-date
					if cut, ok := lcs[id]; ok {
						for i := int32(0); i < s.dataNumReplica; i++ {
							if lc.Cut[i] < cut.Cut[i] {
								valid = false
							}
						}
					}
					if valid {
						lcs[id] = lc

						// update the average delta between consecutive cuts
						if _, ok := s.avgDelta[id]; !ok {
							// if first local cut from that replica
							s.avgDelta[id] = movingaverage.New(int(s.localCutChangeWindow))
							s.quota[id] = lc.Quota
							s.lastCutTime[id] = time.Now()
							s.prevLocalCutNum[id] = lc.LocalCutNum
						} else {
							// if local cut is strictly the next one in order (ignore retries, can occur during shard internal failures)
							if lc.LocalCutNum == (s.prevLocalCutNum[id]+1)%s.localCutChangeWindow {
								delta := time.Since(s.lastCutTime[id]).Nanoseconds()
								s.avgDelta[id].Add(float64(delta))
								s.lastCutTime[id] = time.Now()
								s.prevLocalCutNum[id] = lc.LocalCutNum

								if lc.LocalCutNum == int64(float64(s.localCutChangeWindow)*quotaChangeFraction) {
									// compute new quota for shard if necessary
									avgDelta := s.avgDelta[id].Avg()
									currFreq := float64(1e9 / avgDelta)
									defaultFreq := float64(1e9 / float64(s.batchingInterval.Nanoseconds()))

									// check if frequency changed by more than 10%
									if math.Abs(currFreq-defaultFreq)/defaultFreq >= 0.1 {
										// decided new quota for the next window
										s.quota[id] = int64(float64(lc.Quota) * currFreq / defaultFreq)
									}
									s.quotaChanged++
								}
							}
						}

						if _, ok := lcsBatches[lc.LocalCutNum]; !ok {
							lcsBatches[lc.LocalCutNum] = make(map[int32]*orderpb.LocalCut)
						}
						lcsBatches[lc.LocalCutNum][id] = lc

						// check if the batch is ready
						if len(lcsBatches[lc.LocalCutNum]) == len(liveShardPrimaries) {
							readyBatch <- lc.LocalCutNum
						}
					}
				}
			} else {
				// TODO: forward to the leader
				log.Debugf("Cuts forward to the leader")
			}
		case <-ticker.C:
			// TODO: check to make sure the key in lcs exist
			// log.Debugf("processReport ticker")
			if s.isLeader { // compute committedCut
				batch := <-readyBatch
				ccut := s.computeCommittedCut(lcsBatches[batch])
				vid := atomic.LoadInt32(&s.viewID)
				if s.quotaChanged == int64(len(liveShardPrimaries)) {
					quota := make(map[int32]int64)
					maps.Copy(quota, s.quota)
					ce := &orderpb.CommittedEntry{Seq: 0, ViewID: vid, CommittedCut: &orderpb.CommittedCut{StartGSN: s.startGSN, ShardQuotas: quota, IsShardQuotaUpdated: true, Cut: ccut}, FinalizeShards: nil}
					s.proposeC <- ce
					s.quotaChanged = 0
				} else {
					ce := &orderpb.CommittedEntry{Seq: 0, ViewID: vid, CommittedCut: &orderpb.CommittedCut{StartGSN: s.startGSN, Cut: ccut}, FinalizeShards: nil}
					s.proposeC <- ce
				}

				s.startGSN += s.computeCutDiff(s.prevCut, ccut)
				s.prevCut = ccut
			}
		}
	}
}

// proposeCommit broadcasts entries in commitC to all subCs.
func (s *OrderServer) processCommit() {
	for e := range s.commitC {
		if s.isLeader {
			log.Debugf("%v", e)
		}
		s.subCMu.RLock()
		for _, c := range s.subC {
			c <- e
		}
		s.subCMu.RUnlock()
	}
}

func (s *OrderServer) processRNCommit() {
	for d := range s.rnCommitC {
		if d == nil {
			// TODO: handle snapshots
			continue
		}
		e := &orderpb.CommittedEntry{}
		// TODO use []byte to avoid the conversion
		err := proto.Unmarshal([]byte(*d), e)
		if err != nil {
			log.Errorf("%v", err)
			continue
		}
		s.commitC <- e
	}
}

// TODO
func (s *OrderServer) getSnapshot() ([]byte, error) {
	b := make([]byte, 0)
	return b, nil
}
