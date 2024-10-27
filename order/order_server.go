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

func getLowestWindowNum(lcs map[int32]*orderpb.LocalCut) int64 {
	lowestWindowNum := int64(math.MaxInt64)
	for _, lc := range lcs {
		if lc.WindowNum < lowestWindowNum {
			lowestWindowNum = lc.WindowNum
		}
	}
	return lowestWindowNum
}

func (s *OrderServer) diff(l1, w1, l2, w2 int64) int64 {
	return l1 + s.localCutChangeWindow*w1 - l2 - s.localCutChangeWindow*w2
}

// iterates over all the replicas in the corresponding window number
// returns the lowest local cut number over all replicas in that window (if any replica hasn't reached window yet, returns math.MaxInt64)
func (s *OrderServer) getLowestLocalCutNum(lcs map[int32]*orderpb.LocalCut, windowNum int64) int64 {
	lowestLocalCutNum := int64(math.MaxInt64)

	for rid := range s.quota[windowNum] {
		lc, ok := lcs[rid]
		if !ok || lc.WindowNum < windowNum {
			// if any replica has not reached the window yet, return math.MaxInt64
			return math.MaxInt64
		} else {
			lcNum := lc.LocalCutNum

			// for replicas that have advanced beyond this window, use the highest possible local cut number
			if lc.WindowNum > windowNum {
				lcNum = s.localCutChangeWindow - 1
			}

			if lcNum < lowestLocalCutNum {
				lowestLocalCutNum = lcNum
			}
		}
	}

	return lowestLocalCutNum
}

type Stats struct {
	timeToComputeCommittedCut int64
	numCommittedCuts          int64
	timeToDecideQuota         int64
	numQuotaDecisions         int64
	diffCut                   float64
}

func (s *Stats) printStats() {
	if s.numCommittedCuts == 0 {
		return
	}
	log.Printf("avg time to compute committed cut in us: %v", s.timeToComputeCommittedCut/s.numCommittedCuts/1000)
	log.Printf("avg lag in cuts: %v", s.diffCut/float64(s.numCommittedCuts))
	if s.numQuotaDecisions == 0 {
		return
	}
	log.Printf("avg time to decide quota in us: %v", s.timeToDecideQuota/s.numQuotaDecisions/1000)
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
	avgHoles             map[int32]*movingaverage.MovingAverage
	avgDelta             map[int32]*movingaverage.MovingAverage
	prevQueueLength      map[int32]int64
	quota                map[int64](map[int32]int64) // map from window number to quota for window
	localCutChangeWindow int64
	assignWindow         int64           // next window to assign quota
	numQuotaChanged      int64           // number of primaries in current window for whom quota has 	been changed
	replicasInReserve    map[int32]int64 // replicas in reserve for next window
	replicasStandby      map[int32]int64 // replicas in standby for next window (all replicas from shard not yet registered)
	processWindow        int64           // window number till which quota has been processed
	prevCutTime          map[int32]time.Time

	// stats
	stats Stats
}

// fraction of the local cut window at which quota change decisions are made for a shard
const quotaChangeFraction float64 = 0.5

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
	s.prevCutTime = make(map[int32]time.Time)
	s.avgHoles = make(map[int32]*movingaverage.MovingAverage)
	s.avgDelta = make(map[int32]*movingaverage.MovingAverage)
	s.prevQueueLength = make(map[int32]int64)
	s.quota = make(map[int64]map[int32]int64)
	s.localCutChangeWindow = 100
	s.replicasInReserve = make(map[int32]int64)
	s.replicasStandby = make(map[int32]int64)
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
	s.stats = Stats{}
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
		if lc.WindowNum-1 != windowNum {
			log.Errorf("error: theoretically unreachable code, replicas can only be 1 window apart")
			return -1
		}
		adjustedCut -= (lc.LocalCutNum + 1) * s.quota[lc.WindowNum][rid]
		adjustedCut -= (s.localCutChangeWindow - localCutNum - 1) * s.quota[lc.WindowNum-1][rid]
	} else {
		adjustedCut -= (lc.LocalCutNum - localCutNum) * s.quota[lc.WindowNum][rid]
	}

	// adjust to the given window and local cut number
	return adjustedCut
}

func (s *OrderServer) isReadyToAssignQuota() bool {
	if s.assignWindow == 0 {
		return len(s.replicasInReserve) == 2
	}
	return s.numQuotaChanged == int64(len(s.quota[s.assignWindow-1]))
}

// this function finds the amount of lag in the cuts across all replicas
// the lag is the difference between the cut number of a replica and the highest cut number across all replicas
// where, cut number = windowNumber * localCutChangeWindow + localCutNumber
func (s *OrderServer) getLags(lcs map[int32]*orderpb.LocalCut) map[int32]int64 {
	lags := make(map[int32]int64)
	highestCut := int64(0)
	for _, lc := range lcs {
		cutNum := lc.WindowNum*s.localCutChangeWindow + lc.LocalCutNum
		if cutNum > highestCut {
			highestCut = cutNum
		}
	}
	for rid, lc := range lcs {
		cutNum := lc.WindowNum*s.localCutChangeWindow + lc.LocalCutNum
		lags[rid] = highestCut - cutNum
	}
	return lags
}

// logs the avg lag in cuts and returns true if there is a significant lag in the cuts
// significance is currently defined as a maximum lag of 10% of the localCutChangeWindow or more
func (s *OrderServer) isSignificantLag(lags map[int32]int64) bool {
	maxLag := float64(0)
	for _, lag := range lags {
		if float64(lag) > maxLag {
			maxLag = float64(lag)
		}
	}
	s.stats.diffCut += float64(maxLag)
	return float64(maxLag) >= float64(0.1)*float64(s.localCutChangeWindow)
}

func (s *OrderServer) computeCommittedCut(lcs map[int32]*orderpb.LocalCut) map[int32]int64 {
	// find lowest numbered window across all replicas
	lowestWindowNum := getLowestWindowNum(lcs)

	if lowestWindowNum == math.MaxInt64 {
		return s.prevCut
	}

	// lowestLocalCutNum, s.nextWindowNumber is the highest cuts that can be included in the committed cut across all replicas
	lowestLocalCutNum := s.getLowestLocalCutNum(lcs, lowestWindowNum)

	if lowestLocalCutNum == math.MaxInt64 {
		return s.prevCut
	}

	log.Debugf("Computing committed cut for lcs %v", lcs)
	log.Debugf("lowest window num, local cut num: %v, %v", lowestWindowNum, lowestLocalCutNum)
	ccut := make(map[int32]int64)
	for rid := range lcs {
		// only include replicas that are part of the lowest window number
		if _, ok := s.quota[lowestWindowNum][rid]; ok {
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
	}

	if lowestWindowNum > s.processWindow {
		// TODO: delete old quota and unneccesary state
		// if s.processWindow != -1 {
		// 	delete(s.quota, s.processWindow)
		// }
		s.processWindow = lowestWindowNum
	}

	// log.Printf("process window: %v", s.processWindow)

	return ccut
}

func (s *OrderServer) isLastLagFixed(lastLag map[int32]int64) bool {
	for _, lag := range lastLag {
		if lag != 0 {
			return false
		}
	}
	return true
}

func (s *OrderServer) getNewQuota(rid int32, lc *orderpb.LocalCut) int64 {
	// the general idea here is to have very low tolerance for a higher period and moderate to high tolerance to a lower avg cut period
	// defaultFreq := float64(1e9 / s.batchingInterval.Nanoseconds())
	// currentFreq := float64(1e9 / s.avgDelta[rid].Avg())

	// if (currentFreq-defaultFreq)/defaultFreq >= 0.1 || (defaultFreq-currentFreq)/defaultFreq >= 0.2 {
	// 	newQuota := int64(math.Ceil(float64(lc.Quota) * currentFreq / defaultFreq))
	// 	if newQuota < 1 {
	// 		newQuota = 1
	// 	}
	// 	return newQuota
	// }
	return lc.Quota
}

// proposeCommit broadcasts entries in commitC to all subCs.
func (s *OrderServer) processReport() {
	lcs := make(map[int32]*orderpb.LocalCut) // all local cuts
	printTicker := time.NewTicker(5 * time.Second)
	prevPrintCut := make(map[int32]int64)

	// if I send a lag signal, I should not send another one until the lag is resolved
	// wait for 5 windows before sending another lag signal
	var lastLag map[int32]int64
	numCommittedCuts := int64(0)
	ticker := time.NewTicker(s.batchingInterval)
	var quotaStartTime time.Time
	for {
		select {
		case e := <-s.forwardC:
			// Received a LocalCuts message from one of the replica sets
			// This branch has two tasks, store the local cuts and update the quota for the replica set if necessary
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
						if !e.FixingLag {
							// dividing average by difference in local cut numbers in case some are missing
							if _, ok := s.lastCutTime[id]; ok && !e.FixingLag {
								s.avgDelta[id].Add(float64(time.Since(s.lastCutTime[id]).Nanoseconds()) / float64(s.diff(lc.LocalCutNum, lc.WindowNum, lcs[id].LocalCutNum, lcs[id].WindowNum)))
							}
							s.lastCutTime[id] = time.Now()
						}

						if lc.LocalCutNum >= int64(float64(s.localCutChangeWindow)*quotaChangeFraction) {
							// check if quota has already been assigned for next window
							if _, ok := s.quota[lc.WindowNum+1]; !ok {
								s.quota[lc.WindowNum+1] = make(map[int32]int64)
							}

							if _, ok := s.quota[lc.WindowNum+1][id]; !ok {
								s.quota[lc.WindowNum+1][id] = s.getNewQuota(id, lc)
								s.numQuotaChanged++
							}
						}

						// when replica fixes lag, mark last lag as zero
						if lc.Feedback.FixedLag {
							lastLag[id] = 0
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
			if s.isLeader { // update quota for new replicas
				id := lc.ShardID*s.dataNumReplica + lc.LocalReplicaID
				s.replicasStandby[id] = lc.Quota
				log.Debugf("Replica %v registered with quota %v", id, lc.Quota)

				// check if all replicas from that shard have registered
				allRegistered := true
				for i := int32(0); i < s.dataNumReplica; i++ {
					rid := lc.ShardID*s.dataNumReplica + i
					_, ok := s.replicasStandby[rid]
					allRegistered = allRegistered && ok
				}

				if allRegistered {
					// all replicas from the shard have registered
					for i := int32(0); i < s.dataNumReplica; i++ {
						rid := lc.ShardID*s.dataNumReplica + i
						s.replicasInReserve[rid] = s.replicasStandby[rid]
						delete(s.replicasStandby, rid)
						s.avgHoles[rid] = movingaverage.New(int(s.localCutChangeWindow))
						s.avgDelta[rid] = movingaverage.New(int(s.localCutChangeWindow))
					}
					log.Debugf("Shard %v to be added in next avl window", lc.ShardID)
				}
			}
		case <-ticker.C:
			// TODO: check to make sure the key in lcs exist
			// This thread is responsible for computing the committed cut and proposing it to the Raft node
			if s.isLeader { // compute committedCut
				numCommittedCuts++
				start := time.Now()
				ccut := s.computeCommittedCut(lcs)
				lags := s.getLags(lcs)
				s.stats.timeToComputeCommittedCut += time.Since(start).Nanoseconds()
				s.stats.numCommittedCuts++
				vid := atomic.LoadInt32(&s.viewID)
				var ce *orderpb.CommittedEntry

				if s.isReadyToAssignQuota() {
					if s.assignWindow != 0 {
						s.stats.timeToDecideQuota += time.Since(quotaStartTime).Nanoseconds()
						s.stats.numQuotaDecisions++
					}

					log.Debugf("Deciding quota for window %v", s.assignWindow)
					// reset num quota changed
					s.numQuotaChanged = 0

					// if next window quota does not exist, create it
					if _, ok := s.quota[s.assignWindow]; !ok {
						s.quota[s.assignWindow] = make(map[int32]int64)
					}

					// assign quota to replicas in reserve
					for rid, q := range s.replicasInReserve {
						s.quota[s.assignWindow][rid] = q
					}

					// clear replicas in reserve
					s.replicasInReserve = make(map[int32]int64)

					quota := make(map[int32]int64)
					for rid, q := range s.quota[s.assignWindow] {
						quota[rid] = q
					}

					// increment view ID if new shards
					incrViewId := false
					for rid := range s.quota[s.assignWindow] {
						if _, ok := s.shards[rid/s.dataNumReplica]; !ok {
							incrViewId = true
							s.shards[rid/s.dataNumReplica] = true
						}
					}

					if incrViewId {
						atomic.AddInt32(&s.viewID, 1)
						vid = atomic.LoadInt32(&s.viewID)
					}

					// increment window
					s.assignWindow++

					ce = &orderpb.CommittedEntry{Seq: 0, ViewID: vid, CommittedCut: &orderpb.CommittedCut{StartGSN: s.startGSN, Cut: ccut, ShardQuotas: quota, IsShardQuotaUpdated: true, WindowNum: s.assignWindow - 1, ViewID: vid}, FinalizeShards: nil}
					log.Printf("quota: %v", s.quota[s.assignWindow-1])
				} else {
					ce = &orderpb.CommittedEntry{Seq: 0, ViewID: vid, CommittedCut: &orderpb.CommittedCut{StartGSN: s.startGSN, Cut: ccut}, FinalizeShards: nil}
				}

				if s.isSignificantLag(lags) {
					// log.Printf("significant lag in cuts: %v", lags)
					if lastLag == nil || s.isLastLagFixed(lastLag) {
						ce.CommittedCut.AdjustmentSignal = &orderpb.Control{}
						ce.CommittedCut.AdjustmentSignal.Lag = lags
						lastLag = lags
					}
				}

				s.proposeC <- ce
				s.startGSN += s.computeCutDiff(s.prevCut, ccut)
				s.prevCut = ccut
			}
		case <-printTicker.C:
			for rid, cut := range s.prevCut {
				log.Printf("replica %v: %v [+%v]", rid, cut, cut-prevPrintCut[rid])
			}
			prevPrintCut = make(map[int32]int64)
			for rid, cut := range s.prevCut {
				prevPrintCut[rid] = cut
			}
			s.stats.printStats()
			for rid := range s.avgDelta {
				log.Printf("replica %v: avg delta in ms %v", rid, s.avgDelta[rid].Avg()/1000000)
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
