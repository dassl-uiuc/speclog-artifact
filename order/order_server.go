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

func isHigher(l1, w1, l2, w2 int64) bool {
	if w1 > w2 {
		return true
	}
	return l1 > l2
}

func getLowestWindowNum(lcs map[int32]*orderpb.LocalCut) int64 {
	lowestWindowNum := int64(math.MaxInt64)
	for _, lc := range lcs {
		if lc.WindowNum < lowestWindowNum {
			lowestWindowNum = lc.WindowNum
		}
	}
	return lowestWindowNum
}

func getLowestLocalCutNum(lcs map[int32]*orderpb.LocalCut, lowestWindowNum int64) int64 {
	lowestLocalCutNum := int64(math.MaxInt64)
	for _, lc := range lcs {
		if lc.WindowNum == lowestWindowNum && lc.LocalCutNum < lowestLocalCutNum {
			lowestLocalCutNum = lc.LocalCutNum
		}
	}
	return lowestLocalCutNum
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

	registerC            chan *orderpb.LocalCut // new replicas to be registered
	lastCutTime          map[int32]time.Time
	avgDelta             map[int32]*movingaverage.MovingAverage
	quota                map[int64](map[int32]int64) // map from window number to quota for window
	localCutChangeWindow int64
	assignWindow         int64           // next window to assign quota
	numQuotaChanged      int64           // number of primaries in current window for whom quota has 	been changed
	replicasInReserve    map[int32]int64 // replicas in reserve for next window
	processWindow        int64           // window number being processed by the global cut handler
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
	s.quota = make(map[int64]map[int32]int64)
	s.localCutChangeWindow = 10
	s.replicasInReserve = make(map[int32]int64)
	s.assignWindow = 0
	s.processWindow = -1

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

// helper function to adjust the quota for replica rid in the cut lc to the given window and local cut number
func (s *OrderServer) adjustToMinimums(rid int32, lc *orderpb.LocalCut, windowNum int64, localCutNum int64) int64 {
	localReplicaId := rid % s.dataNumReplica
	adjustedCut := lc.Cut[localReplicaId]
	if lc.WindowNum == windowNum && lc.LocalCutNum == localCutNum {
		return adjustedCut
	}
	if lc.WindowNum > windowNum {
		adjustedCut -= (lc.LocalCutNum + 1) * s.quota[lc.WindowNum][rid]
	}

	if lc.WindowNum-1 != windowNum {
		log.Errorf("error: theoretically unreachable code, can never occur")
		return 0
	}

	// adjust to the given window and local cut number
	adjustedCut -= (s.localCutChangeWindow - lc.LocalCutNum - 1) * s.quota[lc.WindowNum-1][rid]
	return adjustedCut
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

	// find lowest numbered window across all replicas
	lowestWindowNum := getLowestWindowNum(lcs)
	ccut := make(map[int32]int64)
	if lowestWindowNum == int64(math.MaxInt64) {
		// no local cuts have been received
		if incrViewID {
			atomic.AddInt32(&s.viewID, 1)
		}
		return ccut
	}

	// lowestLocalCutNum, lowestWindowNum is the highest cuts that can be included in the committed cut across all replicas
	// find the lowest local cut number for this window
	lowestLocalCutNum := getLowestLocalCutNum(lcs, lowestWindowNum)

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
					chosen = s.adjustToMinimums(rid, cut, lowestWindowNum, lowestLocalCutNum)
				}
			} else {
				chosen = int64(0)
			}
		}
		ccut[rid] = chosen
	}

	if lowestWindowNum > s.processWindow {
		// TODO: delete old quota and unneccesary state
		s.processWindow = lowestWindowNum
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
			// Received a LocalCuts message from one of the replica sets
			// This branch has two tasks, store the local cuts and update the quota for the replica set if necessary
			log.Debugf("processReport forwardC")
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
						if _, ok := lcs[id]; !ok || isHigher(lc.LocalCutNum, lc.WindowNum, lcs[id].LocalCutNum, lcs[id].WindowNum) {
							if _, ok := s.lastCutTime[id]; ok {
								// dividing average by difference in local cut numbers in case some are missing
								s.avgDelta[id].Add(float64(time.Since(s.lastCutTime[id]).Nanoseconds()) / float64(lc.LocalCutNum-lcs[id].LocalCutNum))
							}
							s.lastCutTime[id] = time.Now()

							if lc.LocalCutNum >= int64(float64(s.localCutChangeWindow)*quotaChangeFraction) {
								// check if quota has already been assigned for next window
								if _, ok := s.quota[lc.WindowNum+1]; !ok {
									s.quota[lc.WindowNum+1] = make(map[int32]int64)
								}
								if _, ok := s.quota[lc.WindowNum+1][id]; !ok {
									// compute new quota for shard if necessary
									defaultFreq := float64(1e9 / float64(s.batchingInterval.Nanoseconds()))
									currentFreq := float64(1e9 / s.avgDelta[id].Avg())

									// check if frequency changed by more than 10%
									if math.Abs(currentFreq-defaultFreq)/defaultFreq >= 0.1 {
										// decide new quota for the next window
										s.quota[lc.WindowNum+1][id] = int64(float64(lc.Quota) * currentFreq / defaultFreq)
									}
									s.numQuotaChanged++
								}
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
			// A new replicaset requested to register
			// This branch marks the replicas quota and keeps it as a replica in reserve to be included in the next possible window
			log.Debugf("processReport registerC")
			if s.isLeader { // update quota for new replicas
				id := lc.ShardID*s.dataNumReplica + lc.LocalReplicaID
				s.replicasInReserve[id] = lc.Quota
				s.avgDelta[id] = movingaverage.New(10)
			}
		case <-ticker.C:
			// TODO: check to make sure the key in lcs exist
			// This thread is responsible for computing the committed cut and proposing it to the Raft node
			log.Debugf("processReport ticker")
			if s.isLeader { // compute committedCut
				ccut := s.computeCommittedCut(lcs)
				vid := atomic.LoadInt32(&s.viewID)
				var ce *orderpb.CommittedEntry

				if s.assignWindow == 0 || s.numQuotaChanged == int64(len(s.quota[s.assignWindow-1])) {
					// reset num quota changed
					s.numQuotaChanged = 0

					// if next window quota does not exist, create it
					if _, ok := s.quota[s.assignWindow]; !ok {
						s.quota[s.assignWindow] = make(map[int32]int64)
					}

					// assign quota to replicas in reserve
					for rid, quota := range s.replicasInReserve {
						s.quota[s.assignWindow][rid] = quota
					}

					// clear replicas in reserve
					s.replicasInReserve = make(map[int32]int64)

					quota := make(map[int32]int64)
					for rid, q := range s.quota[s.assignWindow] {
						quota[rid] = q
					}

					// increment window
					s.assignWindow++

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
