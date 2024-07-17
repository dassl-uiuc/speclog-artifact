package order

import (
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

// helper functions
func MoveAndClear(from map[int32]bool, to map[int32]bool) {
	for k := range from {
		to[k] = true
		delete(from, k)
	}
}

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

	registerC               chan *orderpb.LocalCut // new replicas to be registered
	lastCutTime             map[int32]time.Time
	avgDelta                map[int32]*movingaverage.MovingAverage
	quota                   map[int32]int64
	localCutChangeWindow    int64
	numQuotaChanged         int64          // number of primaries for whom quota has been changed
	replicasInReserve       map[int32]bool // replicas in reserve
	replicasInCurrentWindow map[int32]bool // replicas in the current window
	replicasForNextWindow   map[int32]bool // replicas for the next window
	nextWindowSealed        bool
	quotaChanged            map[int32]bool
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

	s.registerC = make(chan *orderpb.LocalCut, 4096)
	s.lastCutTime = make(map[int32]time.Time)
	s.avgDelta = make(map[int32]*movingaverage.MovingAverage)
	s.quota = make(map[int32]int64)
	s.localCutChangeWindow = 10
	s.replicasInReserve = make(map[int32]bool)
	s.replicasInCurrentWindow = make(map[int32]bool)
	s.replicasForNextWindow = make(map[int32]bool)
	s.quotaChanged = make(map[int32]bool)

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

	// find lowest cut num across all replicas in lcs
	lowestCutNum := int64(s.localCutChangeWindow)
	notAllSent := false
	for rid, _ := range s.replicasInCurrentWindow {
		if cut, ok := lcs[rid]; ok {
			lowestCutNum = min(lowestCutNum, cut.LocalCutNum)
		} else {
			notAllSent = true
			break
		}
	}
	ccut := make(map[int32]int64)
	if notAllSent {
		// not all replicas have sent at-least one local cut in this window, no global cut can be computed
		if incrViewID {
			atomic.AddInt32(&s.viewID, 1)
		}
		return ccut
	}
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
					quota := cut.Quota
					cutBase := cut.Cut[localReplicaID] - cut.Quota*(cut.LocalCutNum+1)
					// We can only commit up to the lowest cut num on all replicas
					// Note, it is the replicas responsibility to split across quotas if multiple cuts get batched together
					chosen = min(chosen, cutBase+quota*(lowestCutNum+1))
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
	lcs := make(map[int32]*orderpb.LocalCut) // all local cuts
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
						// check if this is a higher local cut
						if lc.LocalCutNum == 0 || lc.LocalCutNum > lcs[id].LocalCutNum {
							if _, ok := s.lastCutTime[id]; ok {
								// dividing average by difference in local cut numbers in case some are missing
								s.avgDelta[id].Add(float64(time.Since(s.lastCutTime[id]).Nanoseconds()) / float64(lc.LocalCutNum-lcs[id].LocalCutNum))
							}
							s.lastCutTime[id] = time.Now()

							if lc.LocalCutNum >= int64(float64(s.localCutChangeWindow)*quotaChangeFraction) && !s.quotaChanged[id] {
								// compute new quota for shard if necessary
								defaultFreq := float64(1e9 / float64(s.batchingInterval.Nanoseconds()))
								currentFreq := float64(1e9 / s.avgDelta[id].Avg())

								// check if frequency changed by more than 10%
								if math.Abs(currentFreq-defaultFreq)/defaultFreq >= 0.1 {
									// decide new quota for the next window
									s.quota[id] = int64(float64(lc.Quota) * currentFreq / defaultFreq)
								}
								s.numQuotaChanged++
								s.quotaChanged[id] = true
							}
						}

						lcs[id] = lc
					}
				}
			} else {
				// TODO: forward to the leader
				log.Debugf("Cuts forward to the leader")
			}
		case lc := <-s.registerC:
			// log.Debugf("processReport registerC")
			if s.isLeader { // update quota for new replicas
				id := lc.ShardID*s.dataNumReplica + lc.LocalReplicaID
				s.quota[id] = lc.Quota
				s.replicasInReserve[id] = true
				s.avgDelta[id] = movingaverage.New(10)
			}
		case <-ticker.C:
			// TODO: check to make sure the key in lcs exist
			// log.Debugf("processReport ticker")
			if s.isLeader { // compute committedCut
				ccut := s.computeCommittedCut(lcs)
				vid := atomic.LoadInt32(&s.viewID)
				var ce *orderpb.CommittedEntry

				// clear LCS if all local cut numbers have reached window size
				allReached := true
				for _, lc := range lcs {
					allReached = allReached && (lc.LocalCutNum == (s.localCutChangeWindow - 1))
				}
				if allReached {
					for k := range lcs {
						delete(lcs, k)
					}
				}

				if s.numQuotaChanged == int64(len(s.replicasInCurrentWindow)) {
					for k := range s.quotaChanged {
						delete(s.quotaChanged, k)
					}
					s.numQuotaChanged = 0
					quota := make(map[int32]int64)
					MoveAndClear(s.replicasInReserve, s.replicasForNextWindow)
					for k := range s.replicasForNextWindow {
						quota[k] = s.quota[k]
					}
					ce = &orderpb.CommittedEntry{Seq: 0, ViewID: vid, CommittedCut: &orderpb.CommittedCut{StartGSN: s.startGSN, Cut: ccut, ShardQuotas: quota, IsShardQuotaUpdated: true}, FinalizeShards: nil}
				} else {
					ce = &orderpb.CommittedEntry{Seq: 0, ViewID: vid, CommittedCut: &orderpb.CommittedCut{StartGSN: s.startGSN, Cut: ccut}, FinalizeShards: nil}
				}
				s.proposeC <- ce

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
