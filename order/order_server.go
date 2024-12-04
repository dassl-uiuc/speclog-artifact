package order

import (
	"sync"
	"sync/atomic"
	"time"

	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/order/orderpb"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft/raftpb"
)

const reconfigExpt = false
const emulation = false

type RealTimeTput struct {
	count atomic.Int64
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
		log.Printf("[real-time tput]: %v ops/sec", float64(count-prev)/duration.Seconds())
		prev = count
	}
}

type Stats struct {
	RealTimeTput RealTimeTput
}

type OrderServer struct {
	index                   int32
	numReplica              int32
	dataNumReplica          int32
	clientID                int32
	batchingInterval        time.Duration
	isLeader                bool
	startGSN                int64
	viewID                  int32          // use sync/atomic to access viewID
	shards                  map[int32]bool // true for live shards, false for finalized ones
	forwardC                chan *orderpb.LocalCuts
	proposeC                chan *orderpb.CommittedEntry
	commitC                 chan *orderpb.CommittedEntry
	finalizeC               chan *orderpb.FinalizeRequest // changed structure for speclog experiments
	subC                    map[int32]chan *orderpb.CommittedEntry
	subCMu                  sync.RWMutex
	prevCut                 map[int32]int64
	replicasFinalizeStandby map[int32]int64 // replicas in standby for finalization, change for speclog
	stats                   Stats
	shardsFinalized         map[int32]bool
	replicasAddingStandby   map[int32]bool // replicas in standby for adding, change for speclog

	rnConfChangeC      chan raftpb.ConfChange
	rnProposeC         chan string
	rnCommitC          <-chan *string
	rnErrorC           <-chan error
	rnSnapshotterReady <-chan *snap.Snapshotter
}

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
	s.replicasFinalizeStandby = make(map[int32]int64)
	s.replicasAddingStandby = make(map[int32]bool)
	s.stats = Stats{}
	if reconfigExpt || emulation {
		go s.stats.RealTimeTput.LoggerThread()
	}
	s.shardsFinalized = make(map[int32]bool)

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
			s.shardsFinalized[shard] = true
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

	if reconfigExpt || emulation {
		totalCommitted := s.computeCutDiff(s.prevCut, ccut)
		s.stats.RealTimeTput.Add(totalCommitted)
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
					if status, ok := s.shards[lc.ShardID]; ok {
						if !status {
							// this is a finalized shard, reject cuts
							continue
						}
					}
					id := lc.ShardID*s.dataNumReplica + lc.LocalReplicaID
					if _, ok := lcs[id]; !ok {
						s.replicasAddingStandby[id] = true
						allReplicasJoined := true
						for i := int32(0); i < s.dataNumReplica; i++ {
							rid := lc.ShardID*s.dataNumReplica + i
							_, ok := s.replicasAddingStandby[rid]
							allReplicasJoined = allReplicasJoined && ok
						}
						if !allReplicasJoined {
							continue
						}
						log.Printf("Replica %v added", id)
					}

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
					}
				}
			} else {
				// TODO: forward to the leader
				log.Debugf("Cuts forward to the leader")
			}
		case shard := <-s.finalizeC:
			// simply mark the shard as finalized
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
						rid := shard.ShardID*s.dataNumReplica + i
						delete(s.replicasFinalizeStandby, rid)
					}
					s.shards[shard.ShardID] = false
					log.Printf("Shard %v to be finalized", shard.ShardID)
				}
			}
		case <-ticker.C:
			// TODO: check to make sure the key in lcs exist
			// log.Debugf("processReport ticker")
			if s.isLeader { // compute committedCut
				ccut := s.computeCommittedCut(lcs)
				vid := atomic.LoadInt32(&s.viewID)
				// get list of finalized shards
				var finalizeEntry *orderpb.FinalizeEntry = nil
				if len(s.shardsFinalized) > 0 {
					finalizeEntry = &orderpb.FinalizeEntry{ShardIDs: make([]int32, 0)}
					for k := range s.shardsFinalized {
						finalizeEntry.ShardIDs = append(finalizeEntry.ShardIDs, k)
					}
					s.shardsFinalized = make(map[int32]bool)
				}
				ce := &orderpb.CommittedEntry{Seq: 0, ViewID: vid, CommittedCut: &orderpb.CommittedCut{StartGSN: s.startGSN, Cut: ccut}, FinalizeShards: finalizeEntry}
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
			if reconfigExpt {
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
