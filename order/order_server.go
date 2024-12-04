package order

import (
	"math"
	"sort"
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

const reconfigExpt bool = false
const qcExpt bool = false
const lagfixExpt bool = false
const qcEnabled bool = true
const lagfixEnabled bool = true
const lagfixThres float64 = 0.03
const emulation bool = false
const staggeringFactor int64 = 8

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

type RealTimeTput struct {
	count  atomic.Int64
	IsHole bool
}

func (r *RealTimeTput) Add(delta int64) {
	r.count.Add(delta)
}

func (r *RealTimeTput) LoggerThread() {
	duration := 50 * time.Millisecond
	ticker := time.NewTicker(duration)
	prev := int64(0)
	for range ticker.C {
		count := r.count.Load()
		if r.IsHole {
			log.Printf("[real-time total tput]: %v ops/sec", float64(count-prev)/duration.Seconds())
		} else {
			log.Printf("[real-time tput]: %v ops/sec", float64(count-prev)/duration.Seconds())
		}
		prev = count
	}
}

type Stats struct {
	timeToComputeCommittedCut int64
	numCommittedCuts          int64
	timeToDecideQuota         int64
	numQuotaDecisions         int64
	diffCut                   float64
	numLagFixes               map[int32]int64
	RealTimeTput              RealTimeTput
	RealTimeTotalTput         RealTimeTput                // includes holes
	holesFilled               map[int32](map[int64]int64) // number of holes filled by each replica (1st int32) in each local cut id (1st int64)
	quotaStartTime            time.Time
	prevNumCommittedCuts      int64
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
	for rid, num := range s.numLagFixes {
		log.Printf("replica %v: num lag fixes: %v", rid, num)
	}
	log.Printf("num committed cuts: %v [+%v]", s.numCommittedCuts, s.numCommittedCuts-s.prevNumCommittedCuts)
	s.prevNumCommittedCuts = s.numCommittedCuts
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
	finalizeC        chan *orderpb.FinalizeRequest
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
	avgHoles                map[int32]*movingaverage.MovingAverage
	avgDelta                map[int32]*movingaverage.MovingAverage
	prevQueueLength         map[int32]int64
	quota                   map[int64](map[int32]int64) // map from window number to quota for window
	sortedReplicaList       map[int64][]int32
	localCutChangeWindow    int64
	assignWindow            int64           // next window to assign quota
	windowStartGSN          map[int64]int64 // map from window number to start GSN
	numQuotaChanged         int64           // number of primaries in current window for whom quota has 	been changed
	replicasInReserve       map[int32]int64 // replicas in reserve for next window
	replicasStandby         map[int32]int64 // replicas in standby for next window (all replicas from shard not yet registered)
	replicasFinalize        map[int32]int64 // replicas to be finalized
	replicasFinalizeStandby map[int32]int64 // replicas in standby for finalization
	replicasConfirmFinalize map[int32]int64 // replicas waiting to be confirmed finalized
	processWindow           int64           // window number till which quota has been processed
	prevCutTime             map[int32]time.Time
	startCommittedCut       map[int32]int64
	// stats
	stats Stats

	lastLag map[int32]int64

	prevLowestWindowNum   int64
	prevLowestLocalCutNum int64
	prevSeqReached        int64
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
	s.finalizeC = make(chan *orderpb.FinalizeRequest, 4096)
	s.subC = make(map[int32]chan *orderpb.CommittedEntry)

	s.registerC = make(chan *orderpb.LocalCut, 4096)
	s.lastCutTime = make(map[int32]time.Time)
	s.prevCutTime = make(map[int32]time.Time)
	s.avgHoles = make(map[int32]*movingaverage.MovingAverage)
	s.avgDelta = make(map[int32]*movingaverage.MovingAverage)
	s.prevQueueLength = make(map[int32]int64)
	s.quota = make(map[int64]map[int32]int64)
	s.localCutChangeWindow = 100
	if !qcEnabled {
		s.localCutChangeWindow = 1e9
	}
	s.replicasInReserve = make(map[int32]int64)
	s.replicasStandby = make(map[int32]int64)
	s.replicasFinalize = make(map[int32]int64)
	s.replicasFinalizeStandby = make(map[int32]int64)
	s.replicasConfirmFinalize = make(map[int32]int64)
	s.assignWindow = 0
	s.startCommittedCut = make(map[int32]int64)
	s.windowStartGSN = make(map[int64]int64)
	s.processWindow = -1
	s.prevLowestWindowNum = -1
	s.prevLowestLocalCutNum = s.localCutChangeWindow - 1
	s.prevSeqReached = 0
	s.sortedReplicaList = make(map[int64][]int32)

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
	s.stats.RealTimeTput.IsHole = false
	s.stats.RealTimeTotalTput.IsHole = true
	if reconfigExpt || qcExpt || emulation {
		go s.stats.RealTimeTput.LoggerThread()
		go s.stats.RealTimeTotalTput.LoggerThread()
		s.stats.holesFilled = make(map[int32](map[int64]int64))
	}
	s.stats.numLagFixes = make(map[int32]int64)
	s.lastLag = make(map[int32]int64)
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
		return len(s.replicasInReserve) >= 2
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
// significance is currently defined as a maximum lag of 5% of the localCutChangeWindow or more
func (s *OrderServer) isSignificantLag(lags map[int32]int64) bool {
	limit := float64(lagfixThres) * float64(s.localCutChangeWindow)
	if lagfixEnabled && !qcEnabled {
		limit = float64(lagfixThres) * float64(100)
	}
	maxLag := float64(0)
	for _, lag := range lags {
		if float64(lag) > maxLag {
			maxLag = float64(lag)
		}
	}
	s.stats.diffCut += float64(maxLag)
	return float64(maxLag) >= limit
}

func (s *OrderServer) getSignificantLags(lags *map[int32]int64) {
	limit := float64(lagfixThres) * float64(s.localCutChangeWindow)
	if lagfixEnabled && !qcEnabled {
		limit = float64(lagfixThres) * float64(100)
	}
	for rid, lag := range *lags {
		if float64(lag) < limit {
			delete(*lags, rid)
		} else {
			if _, ok := s.stats.numLagFixes[rid]; !ok {
				s.stats.numLagFixes[rid] = 0
			}
			s.stats.numLagFixes[rid]++
		}
	}
}

// for reconfig expt
func (s *OrderServer) getNumHolesCommitted(lowestWindowNum int64, lowestLocalCutNum int64) int64 {
	prevCutIndex := s.prevLowestWindowNum*s.localCutChangeWindow + s.prevLowestLocalCutNum
	currCutIndex := lowestWindowNum*s.localCutChangeWindow + lowestLocalCutNum

	numHolesCommitted := int64(0)
	for i := prevCutIndex + 1; i <= currCutIndex; i++ {
		currentWindow := i / s.localCutChangeWindow
		for rid := range s.quota[currentWindow] {
			if _, ok := s.stats.holesFilled[rid]; !ok {
				log.Errorf("err, cannot happen")
			}
			if _, ok := s.stats.holesFilled[rid][i]; !ok {
				log.Errorf("err, cannot happen")
			}
			numHolesCommitted += s.stats.holesFilled[rid][i]
		}
	}
	return numHolesCommitted
}

func (s *OrderServer) getNumSeqReached(localCutNum int64, windowNum int64, lcs map[int32]*orderpb.LocalCut) []int32 {
	seqReached := make([]int32, 0)
	_, ok := s.quota[windowNum]
	if !ok {
		return seqReached
	}

	for _, rid := range s.sortedReplicaList[windowNum] {
		if lc, ok := lcs[rid]; ok {
			if lc.WindowNum > windowNum || (lc.WindowNum == windowNum && lc.LocalCutNum >= localCutNum) {
				seqReached = append(seqReached, rid)
			} else {
				break
			}
		}
	}

	return seqReached
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

	// try to see if we can commit some staggered cuts from the next local cut num
	nextLocalCutNum := lowestLocalCutNum + 1
	nextWindowNum := lowestWindowNum
	if nextLocalCutNum == s.localCutChangeWindow {
		nextLocalCutNum = 0
		nextWindowNum++
	}

	// number of sequential replicas that have reached the next local cut number
	seqReached := s.getNumSeqReached(nextLocalCutNum, nextWindowNum, lcs)
	log.Debugf("seqReached: %v", seqReached)
	lenSeqReached := int64(len(seqReached))
	validCommitLen := (lenSeqReached / staggeringFactor) * staggeringFactor
	log.Debugf("committing upto local cut num %v, window num %v, validCommitLen %v for lcs %v", lowestLocalCutNum, lowestWindowNum, validCommitLen, lcs)
	for i := int64(0); i < validCommitLen; i++ {
		ccut[seqReached[i]] = s.adjustToMinimums(seqReached[i], lcs[seqReached[i]], nextWindowNum, nextLocalCutNum)
	}

	// check if lcs num is equal to localCutChangeWindow-1
	// If so we can clear some state
	// WILL NEVER REACH THE BELOW CODE IN EMULATION, IGNORE
	removableReplicas := make([]int32, 0)
	for rid := range lcs {
		wn, ok := s.replicasConfirmFinalize[rid]
		if !ok {
			continue
		}

		log.Debugf("Wn: %v, lowestWindowNum: %v, lowestLocalCutNum: %v", wn, lowestWindowNum, lowestLocalCutNum)

		if wn == lowestWindowNum && lowestLocalCutNum == s.localCutChangeWindow-1 {
			// TODO: redundant check, delete this later
			if _, ok := s.replicasConfirmFinalize[rid]; ok {
				removableReplicas = append(removableReplicas, rid)
			}
		}
	}

	for _, rid := range removableReplicas {
		delete(s.avgHoles, rid)
		delete(s.avgDelta, rid)
		delete(s.replicasConfirmFinalize, rid)
		delete(lcs, rid)
		if reconfigExpt {
			log.Printf("Replica %v finalized", rid) // uncomment for reconfig expt
		}
	}

	// uncomment for reconfig expt
	// update records stat
	if reconfigExpt || qcExpt || emulation {
		totalCommitted := s.computeCutDiff(s.prevCut, ccut)
		// RETHINK THIS FOR STAGGERRED CUTS
		holesCommitted := s.getNumHolesCommitted(lowestWindowNum, lowestLocalCutNum)
		s.stats.RealTimeTput.Add(totalCommitted - holesCommitted)
		s.stats.RealTimeTotalTput.Add(totalCommitted)
	}

	s.prevLowestLocalCutNum = lowestLocalCutNum
	s.prevLowestWindowNum = lowestWindowNum
	s.prevSeqReached = validCommitLen

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

// if I send a lag signal, I should not send another one until the lag is resolved
func (s *OrderServer) isLastLagFixed() bool {
	for _, lag := range s.lastLag {
		if lag != 0 {
			return false
		}
	}
	return true
}

func (s *OrderServer) getNewQuota(rid int32, lc *orderpb.LocalCut) int64 {
	// the general idea here is to have very low tolerance for a higher period and moderate to high tolerance to a lower avg cut period
	if qcEnabled {
		defaultFreq := float64(1e9 / s.batchingInterval.Nanoseconds())
		currentFreq := float64(1e9 / s.avgDelta[rid].Avg())

		if math.Abs(currentFreq-defaultFreq) > 0.05*defaultFreq {
			newQuota := int64(math.Ceil(float64(lc.Quota) * currentFreq / defaultFreq))
			if newQuota < 1 {
				newQuota = 1
			}
			return newQuota
		}
	}
	return lc.Quota
}

func (s *OrderServer) evalCommittedCut(lcs map[int32]*orderpb.LocalCut) {
	if s.isLeader { // compute committedCut
		start := time.Now()
		ccut := s.computeCommittedCut(lcs)
		lags := s.getLags(lcs)
		s.stats.timeToComputeCommittedCut += time.Since(start).Nanoseconds()
		s.stats.numCommittedCuts++
		vid := atomic.LoadInt32(&s.viewID)
		var ce *orderpb.CommittedEntry

		if s.isReadyToAssignQuota() {
			if s.assignWindow != 0 {
				s.stats.timeToDecideQuota += time.Since(s.stats.quotaStartTime).Nanoseconds()
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

			shardsFinalized := false
			finalizeEntry := &orderpb.FinalizeEntry{ShardIDs: make([]int32, 0)}
			// iterate over s.replicasFinalize and remove them from the quota for the next window
			for rid := range s.replicasFinalize {
				delete(s.quota[s.assignWindow], rid)
				s.replicasConfirmFinalize[rid] = s.assignWindow - 1
				shardsFinalized = true

				found := false
				for _, sid := range finalizeEntry.ShardIDs {
					if sid == rid/s.dataNumReplica {
						found = true
						break
					}
				}
				if !found {
					finalizeEntry.ShardIDs = append(finalizeEntry.ShardIDs, rid/s.dataNumReplica)
				}
			}

			// clear replicas in finalize
			s.replicasFinalize = make(map[int32]int64)

			// clear replicas in reserve
			s.replicasInReserve = make(map[int32]int64)

			quota := make(map[int32]int64)
			s.sortedReplicaList[s.assignWindow] = make([]int32, 0)
			for rid, q := range s.quota[s.assignWindow] {
				quota[rid] = q
				s.sortedReplicaList[s.assignWindow] = append(s.sortedReplicaList[s.assignWindow], rid)
			}

			sort.Slice(s.sortedReplicaList[s.assignWindow], func(i, j int) bool {
				return s.sortedReplicaList[s.assignWindow][i] < s.sortedReplicaList[s.assignWindow][j]
			})

			// increment view ID if new shards
			// increment view ID if shards leave as well
			incrViewId := false
			for rid := range s.quota[s.assignWindow] {
				if _, ok := s.shards[rid/s.dataNumReplica]; !ok {
					log.Printf("Incrementing view ID as new shard added")
					incrViewId = true
					s.shards[rid/s.dataNumReplica] = true
				}
			}

			if incrViewId || shardsFinalized {
				log.Printf("Incrementing view ID because shardFinalized: %v, incrViewId: %v", shardsFinalized, incrViewId)
				atomic.AddInt32(&s.viewID, 1)
				vid = atomic.LoadInt32(&s.viewID)
			}

			// update window start GSN
			if s.assignWindow == 0 {
				s.windowStartGSN[s.assignWindow] = 0
			} else {
				sumQuotas := 0
				for _, q := range s.quota[s.assignWindow-1] {
					sumQuotas += int(q)
				}
				s.windowStartGSN[s.assignWindow] = s.windowStartGSN[s.assignWindow-1] + int64(sumQuotas)*s.localCutChangeWindow
			}

			// update start committed cut
			// this will be considered by the newly starting replicas to be the expected previous committed cut
			for rid := range s.quota[s.assignWindow] {
				if s.assignWindow == 0 {
					s.startCommittedCut[rid] = 0
				} else {
					_, ok := s.quota[s.assignWindow-1][rid]
					if ok {
						s.startCommittedCut[rid] = s.startCommittedCut[rid] + s.quota[s.assignWindow-1][rid]*s.localCutChangeWindow
					} else {
						s.startCommittedCut[rid] = 0
					}
				}
			}

			for rid := range s.startCommittedCut {
				if _, ok := s.quota[s.assignWindow][rid]; !ok {
					delete(s.startCommittedCut, rid)
				}
			}

			prevCutHint := make(map[int32]int64)
			for rid, cut := range s.startCommittedCut {
				prevCutHint[rid] = cut
			}

			// increment window
			s.assignWindow++

			ce = &orderpb.CommittedEntry{Seq: 0, ViewID: vid, CommittedCut: &orderpb.CommittedCut{StartGSN: s.startGSN, Cut: ccut, ShardQuotas: quota, IsShardQuotaUpdated: true, WindowNum: s.assignWindow - 1, ViewID: vid, WindowStartGSN: s.windowStartGSN[s.assignWindow-1], PrevCut: prevCutHint}, FinalizeShards: finalizeEntry}
			log.Printf("quota: %v", s.quota[s.assignWindow-1])
		} else {
			ce = &orderpb.CommittedEntry{Seq: 0, ViewID: vid, CommittedCut: &orderpb.CommittedCut{StartGSN: s.startGSN, Cut: ccut}, FinalizeShards: nil}
		}

		if lagfixEnabled {
			if s.isSignificantLag(lags) {
				if s.lastLag == nil || s.isLastLagFixed() {
					if lagfixExpt {
						log.Printf("significant lag in cuts: %v", lags)
					}
					ce.CommittedCut.AdjustmentSignal = &orderpb.Control{}
					s.getSignificantLags(&lags)
					ce.CommittedCut.AdjustmentSignal.Lag = lags
					s.lastLag = lags
				}
			}
		}

		s.proposeC <- ce
		s.startGSN += s.computeCutDiff(s.prevCut, ccut)
		s.prevCut = ccut
	}
}

// proposeCommit broadcasts entries in commitC to all subCs.
func (s *OrderServer) processReport() {
	lcs := make(map[int32]*orderpb.LocalCut) // all local cuts
	printTicker := time.NewTicker(5 * time.Second)
	prevPrintCut := make(map[int32]int64)

	// worst-case periodic committed cuts are triggered by this timer
	worstCasePeriod := time.Duration(1.5 * float64(s.batchingInterval))
	timer := time.NewTimer(worstCasePeriod)
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
						if lagfixExpt || qcExpt {
							log.Printf("%v", lc)
						}
						if !e.FixingLag {
							// dividing average by difference in local cut numbers in case some are missing
							if _, ok := s.lastCutTime[id]; ok {
								numCuts := s.diff(lc.LocalCutNum, lc.WindowNum, lcs[id].LocalCutNum, lcs[id].WindowNum)
								s.avgDelta[id].Add(float64(time.Since(s.lastCutTime[id]).Nanoseconds()) / float64(numCuts))
							}
							s.lastCutTime[id] = time.Now()
						}

						if lc.LocalCutNum >= int64(float64(s.localCutChangeWindow)*quotaChangeFraction) && s.assignWindow == lc.WindowNum+1 {
							// check if quota has already been assigned for next window
							if _, ok := s.quota[lc.WindowNum+1]; !ok {
								s.quota[lc.WindowNum+1] = make(map[int32]int64)
							}

							if _, ok := s.quota[lc.WindowNum+1][id]; !ok {
								s.quota[lc.WindowNum+1][id] = s.getNewQuota(id, lc)
								s.numQuotaChanged++
								if s.numQuotaChanged == 1 {
									s.stats.quotaStartTime = time.Now()
								}
							}
						}

						// when replica fixes lag, mark last lag as zero
						if lc.Feedback.FixedLag {
							s.lastLag[id] = 0
						}

						lcs[id] = lc

						if reconfigExpt || qcExpt || emulation {
							// update hole stats
							if _, ok := s.stats.holesFilled[id]; !ok {
								s.stats.holesFilled[id] = make(map[int64]int64)
							}
							s.stats.holesFilled[id][lc.LocalCutNum+lc.WindowNum*s.localCutChangeWindow] = lc.Feedback.NumHoles
						}
					}
				}

				// try to see if I can commit a cut
				lowestWindowNum := getLowestWindowNum(lcs)
				if lowestWindowNum == math.MaxInt64 {
					continue
				}
				lowestLocalCutNum := s.getLowestLocalCutNum(lcs, lowestWindowNum)
				if lowestLocalCutNum == math.MaxInt64 {
					continue
				}

				nextLocalCutNum := lowestLocalCutNum + 1
				nextWindowNum := lowestWindowNum
				if nextLocalCutNum == s.localCutChangeWindow {
					nextLocalCutNum = 0
					nextWindowNum++
				}
				seqReached := s.getNumSeqReached(nextLocalCutNum, nextWindowNum, lcs)
				validCutLen := (int64(len(seqReached)) / staggeringFactor) * staggeringFactor
				if lowestWindowNum != s.prevLowestWindowNum || lowestLocalCutNum != s.prevLowestLocalCutNum || validCutLen > s.prevSeqReached {
					// if lowestWindowNum != s.prevLowestWindowNum || lowestLocalCutNum != s.prevLowestLocalCutNum {
					// log.Printf("Lowest window num, local cut num: %v, %v", lowestWindowNum, lowestLocalCutNum)
					s.evalCommittedCut(lcs)

					if !timer.Stop() {
						<-timer.C
					}
					timer.Reset(worstCasePeriod)
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
				log.Printf("Replica %v registered with quota %v", id, lc.Quota)

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
					log.Printf("Shard %v to be added in next avl window", lc.ShardID)
				}
			}
		case shard := <-s.finalizeC:
			// A replica set requested to finalize
			// This branch marks the replicas as finalized and removes them from the quota
			if s.isLeader { // finalize replicas
				id := shard.ShardID*s.dataNumReplica + shard.LocalReplicaID
				s.replicasFinalizeStandby[id] = 0
				log.Printf("Replica %v requested to finalize", id)
				// check if all replicas from that shard have requested to finalize
				allRequestedToFinalize := true
				for i := int32(0); i < s.dataNumReplica; i++ {
					rid := shard.ShardID*s.dataNumReplica + i
					_, ok := s.replicasFinalizeStandby[rid]
					allRequestedToFinalize = allRequestedToFinalize && ok
				}
				if allRequestedToFinalize {
					// all replicas from the shard have requested to finalize
					for i := int32(0); i < s.dataNumReplica; i++ {
						log.Debugf("Replica %v requested to finalize", shard.ShardID*s.dataNumReplica+i)
						rid := shard.ShardID*s.dataNumReplica + i
						s.replicasFinalize[rid] = s.replicasFinalizeStandby[rid]
						delete(s.replicasFinalizeStandby, rid)
					}
					log.Printf("Shard %v to be finalized in next avl window", shard.ShardID)
				}
			}
		case <-timer.C:
			// TODO: check to make sure the key in lcs exist
			// This thread is responsible for computing the committed cut and proposing it to the Raft node
			s.evalCommittedCut(lcs)
			timer.Reset(worstCasePeriod)
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
			if lagfixExpt || reconfigExpt || qcExpt {
				log.Printf("%v", e)
			} else {
				log.Debugf("%v", e)
			}
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
