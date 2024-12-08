package data

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scalog/scalog/data/datapb"
	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/order/orderpb"
	"github.com/scalog/scalog/pkg/address"
	"github.com/scalog/scalog/storage"
	"google.golang.org/grpc"
)

const backendOnly = false
const measureOrderingInterval = false
const reconfigExpt bool = false
const lagfixExpt bool = false
const qcExpt bool = false

var qcBurstLSN int64 = -1
var qcBurstLocalCutNumber int64 = -1

type clientSubscriber struct {
	state    clientSubscriberState
	respChan chan *datapb.Record
	startGsn int64
}

type clientSubscriberState int

const (
	BEHIND  clientSubscriberState = 0
	UPDATED clientSubscriberState = 1
	CLOSED  clientSubscriberState = 2
)

type DataServer struct {
	// data s configurations
	shardID          int32
	replicaID        int32
	numReplica       int32
	batchingInterval time.Duration
	// s state
	clientID         int32 // incremental counter to distinguish clients
	viewID           int32
	localCut         []int64
	localCutMu       sync.Mutex
	prevCommittedCut *orderpb.CommittedCut
	// ordering layer information
	orderAddr   address.OrderAddr
	orderConn   *grpc.ClientConn
	orderClient *orderpb.Order_ReportClient
	orderMu     sync.RWMutex
	// peer information
	dataAddr    address.DataAddr
	peerConns   []*grpc.ClientConn
	peerClients []*datapb.Data_ReplicateClient
	peerDoneC   []chan interface{}
	// channels used to communate with clients, peers, and ordering layer
	appendC                  chan *datapb.Record
	replicateC               chan *datapb.Record
	selfReplicateC           chan *datapb.Record
	replicateSendC           []chan *datapb.Record
	ackC                     chan *datapb.Ack
	ackSendC                 map[int32]chan *datapb.Ack
	ackSendCMu               sync.RWMutex
	clientSubscribers        []*clientSubscriber
	clientSubscribersMu      sync.Mutex
	newClientSubscribersChan chan *clientSubscriber
	liveSubscribeC           chan int64
	subWg                    sync.WaitGroup

	prevSentLocalCut  int64
	localCglobalCSync chan bool

	committedRecords   map[int64]*datapb.Record
	committedRecordsMu sync.RWMutex

	storage *storage.Storage

	wait   map[int64]chan *datapb.Ack
	waitMu sync.RWMutex

	committedEntryC chan *orderpb.CommittedEntry

	// stores records directly sent to this replica
	records   map[int64]*datapb.Record
	recordsMu sync.RWMutex

	timeOfEntry              map[int64]time.Time
	avgOrderingLatencyMicros float64
	numOrders                int64

	// wait group to confirm the replication of records
	replicateConfWg map[int64]*sync.WaitGroup
	replicateConfMu sync.RWMutex

	// written to disk
	diskWriteC  map[int64]chan bool
	diskWriteMu sync.RWMutex

	replStartTime map[int64]time.Time
	replMu        sync.Mutex

	recordsInPipeline atomic.Int64
	stopReport        chan bool

	localCutNum int64
}

func NewDataServer(replicaID, shardID, numReplica int32, batchingInterval time.Duration, dataAddr address.DataAddr, orderAddr address.OrderAddr) *DataServer {
	var err error
	s := &DataServer{
		replicaID:        replicaID,
		shardID:          shardID,
		clientID:         0,
		viewID:           0,
		numReplica:       numReplica,
		batchingInterval: batchingInterval,
		orderAddr:        orderAddr,
		dataAddr:         dataAddr,
	}
	s.localCut = make([]int64, numReplica)
	for i := 0; i < int(numReplica); i++ {
		s.localCut[i] = 0
	}
	// initialize basic data structures
	s.committedEntryC = make(chan *orderpb.CommittedEntry, 4096)
	s.ackSendC = make(map[int32]chan *datapb.Ack)
	s.ackC = make(chan *datapb.Ack, 4096)
	s.appendC = make(chan *datapb.Record, 8192)
	s.selfReplicateC = make(chan *datapb.Record, 4096)
	s.replicateC = make(chan *datapb.Record, 4096)
	s.replicateSendC = make([]chan *datapb.Record, numReplica)
	s.peerDoneC = make([]chan interface{}, numReplica)
	s.wait = make(map[int64]chan *datapb.Ack)
	s.prevCommittedCut = &orderpb.CommittedCut{}
	s.records = make(map[int64]*datapb.Record)
	s.replicateConfWg = make(map[int64]*sync.WaitGroup)
	s.diskWriteC = make(map[int64]chan bool)
	s.localCglobalCSync = make(chan bool, 1)
	s.prevSentLocalCut = 0
	s.newClientSubscribersChan = make(chan *clientSubscriber, 4096)
	s.liveSubscribeC = make(chan int64, 4096)
	s.subWg = sync.WaitGroup{}
	s.committedRecords = make(map[int64]*datapb.Record)
	s.timeOfEntry = make(map[int64]time.Time)
	s.numOrders = 0
	s.avgOrderingLatencyMicros = 0
	s.replStartTime = make(map[int64]time.Time)
	s.recordsInPipeline.Store(0)
	s.stopReport = make(chan bool, 1)
	s.localCutNum = 0

	path := fmt.Sprintf("/data/storage-%v-%v", shardID, replicaID) // TODO configure path
	segLen := int32(1000)                                          // TODO configurable segment length
	storage, err := storage.NewStorage(path, replicaID, numReplica, segLen)
	if err != nil {
		log.Fatalf("Create storage failed: %v", err)
	}
	s.storage = storage
	for i := int32(0); i < numReplica; i++ {
		if i != replicaID {
			s.replicateSendC[i] = make(chan *datapb.Record, 4096)
		}
	}
	for i := 0; i < 100; i++ {
		err = s.UpdateOrder()
		if err == nil {
			break
		}
		log.Warningf("%v", err)
		time.Sleep(time.Second)
	}
	if err != nil {
		log.Errorf("%v", err)
		return nil
	}
	return s
}

func (s *DataServer) UpdateOrderAddr(addr string) error {
	s.orderMu.Lock()
	defer s.orderMu.Unlock()
	if s.orderConn != nil {
		s.orderConn.Close()
	}
	opts := []grpc.DialOption{grpc.WithInsecure(), grpc.WithBlock()}
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return fmt.Errorf("Dial peer %v failed: %v", addr, err)
	}
	s.orderConn = conn
	orderClient := orderpb.NewOrderClient(conn)
	orderReportClient, err := orderClient.Report(context.Background())
	if err != nil {
		return fmt.Errorf("Create replicate client to %v failed: %v", addr, err)
	}
	s.orderClient = &orderReportClient
	return nil
}

func (s *DataServer) UpdateOrder() error {
	addr := s.orderAddr.Get()
	if addr == "" {
		return fmt.Errorf("Wrong order-addr format: %v", addr)
	}
	return s.UpdateOrderAddr(addr)
}

// UpdatePeers updates the peer list of the shard. It should be called only at
// the initialization phase of the s.
// TODO make the list updatable when running
func (s *DataServer) ConnPeers() error {
	// create connections with peers
	s.peerConns = make([]*grpc.ClientConn, s.numReplica)
	s.peerClients = make([]*datapb.Data_ReplicateClient, s.numReplica)
	for i := int32(0); i < s.numReplica; i++ {
		if i == s.replicaID {
			continue
		}
		err := s.connectToPeer(i)
		if err != nil {
			log.Errorf("%v", err)
			return err
		}
		done := make(chan interface{})
		sendC := s.replicateSendC[i]
		client := s.peerClients[i]
		s.peerDoneC[i] = done
		go s.replicateRecords(done, sendC, client)
		go s.confirmReplication(client)
	}
	return nil
}

func (s *DataServer) Start() {
	for i := 0; i < 100; i++ {
		err := s.ConnPeers()
		if err != nil {
			log.Errorf("%v", err)
			time.Sleep(time.Second)
			continue
		}
		go s.processAppend()
		go s.processReplicate()
		go s.processSelfReplicate()
		go s.processAck()
		go s.processCommittedEntry()
		go s.reportLocalCut()
		go s.receiveCommittedCut()
		go s.processNewSubscribers()
		go s.processLiveSubscribe()
		if reconfigExpt {
			go s.finalizeShardStandby()
		}
		return
	}
	log.Errorf("Error creating data s sid=%v,rid=%v", s.shardID, s.replicaID)
}

func (s *DataServer) processNewSubscribers() {
	for sub := range s.newClientSubscribersChan {
		s.clientSubscribersMu.Lock()
		s.clientSubscribers = append(s.clientSubscribers, sub)
		s.clientSubscribersMu.Unlock()
	}
}

func (s *DataServer) sendToSubscriber(sub *clientSubscriber, gsn int64) {
	defer s.subWg.Done()
	if sub.state == CLOSED {
		return
	}
	startRecords := gsn
	endRecords := gsn
	if sub.state == BEHIND {
		startRecords = sub.startGsn
	}
	s.committedRecordsMu.RLock()
	for i := startRecords; i <= endRecords; i++ {
		if record, ok := s.committedRecords[i]; ok {
			sub.respChan <- record
		}
	}
	s.committedRecordsMu.RUnlock()
	if sub.state == BEHIND {
		sub.state = UPDATED
	}
}

func (s *DataServer) processLiveSubscribe() {
	var latestGsn int64
	latestGsn = -1
	for {
		select {
		case gsn := <-s.liveSubscribeC:
			latestGsn = gsn
			s.clientSubscribersMu.Lock()
			numSub := len(s.clientSubscribers)
			s.subWg.Add(numSub)
			for _, sub := range s.clientSubscribers {
				if sub == nil {
					s.subWg.Done()
					continue
				}
				if sub.state != CLOSED && gsn >= sub.startGsn {
					go s.sendToSubscriber(sub, gsn)
				} else {
					s.subWg.Done()
				}
			}
			s.clientSubscribersMu.Unlock()
			s.subWg.Wait()
		case <-time.After(10 * time.Millisecond):
			// update only those behind upto the latestGsn received so far
			if latestGsn == -1 {
				continue
			}
			s.clientSubscribersMu.Lock()
			numSub := len(s.clientSubscribers)
			s.subWg.Add(numSub)
			for _, sub := range s.clientSubscribers {
				if sub == nil {
					s.subWg.Done()
					continue
				}
				if sub.state == BEHIND && latestGsn >= sub.startGsn {
					go s.sendToSubscriber(sub, latestGsn)
				} else {
					s.subWg.Done()
				}
			}
			s.clientSubscribersMu.Unlock()
			s.subWg.Wait()
		}
	}
}

func (s *DataServer) connectToPeer(peer int32) error {
	// do not connect to the node itself
	if peer == s.replicaID {
		return nil
	}
	// close existing network connections if they exist
	if s.peerConns[peer] != nil {
		s.peerConns[peer].Close()
		s.peerConns[peer] = nil
	}
	if s.peerDoneC[peer] != nil {
		close(s.peerDoneC[peer])
		s.peerDoneC[peer] = nil
	}
	// build connections to peers
	opts := []grpc.DialOption{grpc.WithInsecure(), grpc.WithBlock()}
	var conn *grpc.ClientConn
	var err error
	for i := 0; i < 100; i++ {
		conn, err = grpc.Dial(s.dataAddr.Get(s.shardID, peer), opts...)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return fmt.Errorf("Dial peer %v failed: %v", s.dataAddr.Get(s.shardID, peer), err)
	}
	s.peerConns[peer] = conn
	dataClient := datapb.NewDataClient(conn)
	callOpts := []grpc.CallOption{}
	replicateSendClient, err := dataClient.Replicate(context.Background(), callOpts...)
	if err != nil {
		return fmt.Errorf("Create replicate client to %v failed: %v", s.dataAddr.Get(s.shardID, peer), err)
	}
	s.peerClients[peer] = &replicateSendClient
	return nil
}

func (s *DataServer) replicateRecords(done <-chan interface{}, ch chan *datapb.Record, client *datapb.Data_ReplicateClient) {
	for {
		select {
		case record := <-ch:
			// log.Debugf("Data %v,%v send", s.shardID, s.replicaID)
			err := (*client).Send(record)
			if err != nil {
				log.Errorf("Send record error: %v", err)
			}
		case <-done:
			return
		}
	}
}

func (s *DataServer) confirmReplication(client *datapb.Data_ReplicateClient) {
	for {
		ack, err := (*client).Recv()
		if err != nil {
			log.Errorf("Receive replication acknowledgement error: %v", err)
			return
		}
		id := int64(ack.ClientID)<<32 + int64(ack.ClientSN)
		s.replicateConfMu.RLock()
		s.replicateConfWg[id].Done()
		s.replicateConfMu.RUnlock()
	}
}

// processAppend sends records to replicateC and replicates them to peers
func (s *DataServer) processAppend() {
	for record := range s.appendC {
		record.LocalReplicaID = s.replicaID
		record.ShardID = s.shardID
		// create wait group to confirm the replication of the record
		id := int64(record.ClientID)<<32 + int64(record.ClientSN)
		s.replicateConfMu.Lock()
		s.replicateConfWg[id] = &sync.WaitGroup{}
		s.replicateConfWg[id].Add(int(s.numReplica - 1))
		s.replicateConfMu.Unlock()

		s.recordsInPipeline.Add(1)

		s.replMu.Lock()
		s.replStartTime[id] = time.Now()
		s.replMu.Unlock()

		for i, c := range s.replicateSendC {
			if int32(i) != s.replicaID {
				log.Debugf("Data forward to %v", i)
				c <- record
			}
		}
		s.selfReplicateC <- record
	}
}

// processReplicate writes my peers records to local storage
func (s *DataServer) processReplicate() {
	for record := range s.replicateC {
		// log.Debugf("Data %v,%v process", s.shardID, s.replicaID)
		_, err := s.storage.WriteToPartition(record.LocalReplicaID, record.Record)
		if err != nil {
			log.Fatalf("Write to storage failed: %v", err)
		}

		s.diskWriteMu.RLock()
		id := int64(record.ClientID)<<32 + int64(record.ClientSN)
		if c, ok := s.diskWriteC[id]; ok {
			c <- true
		}
		s.diskWriteMu.RUnlock()
		s.localCutMu.Lock()
		s.localCut[record.LocalReplicaID] = 0
		s.localCutMu.Unlock()
	}
}

// processSelfReplicate writes my own records to local storage
func (s *DataServer) processSelfReplicate() {
	for record := range s.selfReplicateC {
		// log.Debugf("Data %v,%v process", s.shardID, s.replicaID)
		lsn, err := s.storage.WriteToPartition(record.LocalReplicaID, record.Record)
		if err != nil {
			log.Fatalf("Write to storage failed: %v", err)
		}

		// wait for confirmation from all peers
		s.replicateConfMu.RLock()
		recordID := int64(record.ClientID)<<32 + int64(record.ClientSN)
		wg := s.replicateConfWg[recordID]
		s.replicateConfMu.RUnlock()
		wg.Wait()
		s.replicateConfMu.Lock()
		s.replicateConfWg[recordID] = nil
		delete(s.replicateConfWg, recordID)
		s.replicateConfMu.Unlock()

		if backendOnly {
			id := int64(record.ClientID)<<32 + int64(record.ClientSN)
			s.waitMu.RLock()
			c, ok := s.wait[id]
			s.waitMu.RUnlock()
			if ok {
				ack := &datapb.Ack{
					ClientID:       record.ClientID,
					ClientSN:       record.ClientSN,
					ShardID:        s.shardID,
					LocalReplicaID: s.replicaID,
					ViewID:         s.viewID,
					GlobalSN:       0,
				}
				c <- ack
			}
		}

		s.recordsMu.Lock()
		s.records[lsn] = record
		s.recordsMu.Unlock()

		s.localCutMu.Lock()
		if !backendOnly {
			s.localCut[record.LocalReplicaID] = lsn + 1
		}
		s.localCutMu.Unlock()

		s.recordsInPipeline.Add(-1)

		s.replMu.Lock()
		log.Debugf("replication latency: %v", time.Since(s.replStartTime[recordID]).Nanoseconds())
		s.replMu.Unlock()

		if lagfixExpt {
			if record.Record[0] == 'b' {
				log.Printf("burst record with lsn %v", lsn)
			}
		}

		if qcExpt {
			if record.Record[0] == 'b' && qcBurstLSN == -1 {
				qcBurstLSN = lsn
				log.Printf("first burst record stored with lsn %v", lsn)
			}
		}
	}
}

func (s *DataServer) processAck() {
	for ack := range s.ackC {
		// send to ack channel
		s.ackSendCMu.RLock()
		c, ok := s.ackSendC[ack.ClientID]
		s.ackSendCMu.RUnlock()
		if ok {
			c <- ack
		}
		// send individual ack message if requested to block
		id := int64(ack.ClientID)<<32 + int64(ack.ClientSN)
		s.waitMu.RLock()
		c, ok = s.wait[id]
		s.waitMu.RUnlock()
		if ok {
			c <- ack
		}
	}
}

// for reconfig expt
func (s *DataServer) finalizeShardStandby() {
	if s.shardID == 1 {
		// Sleep for 30 seconds
		time.Sleep(30 * time.Second)
		// finalize shard
		s.finalizeShard()
	}
}

func (s *DataServer) finalizeShard() {
	orderClient := orderpb.NewOrderClient(s.orderConn)
	shard := &orderpb.FinalizeRequest{
		ShardID:        s.shardID,
		LocalReplicaID: s.replicaID,
	}
	maxRetries := 5
	for i := 0; i < maxRetries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := orderClient.Finalize(ctx, shard)
		if err == nil {
			fmt.Println("Successfully finalized shard")
			return
		}
		fmt.Printf("Finalize shard failed: %v\n", err)
		// Wait before retrying
		backoff := time.Duration((1 << i)) * time.Second
		fmt.Printf("Retrying in %v...\n", backoff)
		time.Sleep(backoff)
	}
	log.Errorf("Max retries reached. Finalize failed.")
}

func (s *DataServer) reportLocalCut() {
	tick := time.NewTicker(s.batchingInterval)
	prevCut := int64(0)
	for {
		select {
		case <-tick.C:
			lcs := &orderpb.LocalCuts{}
			lcs.Cuts = make([]*orderpb.LocalCut, 1)
			lcs.Cuts[0] = &orderpb.LocalCut{
				ShardID:        s.shardID,
				LocalReplicaID: s.replicaID,
			}
			s.localCutMu.Lock()
			lcs.Cuts[0].Cut = make([]int64, len(s.localCut))
			copy(lcs.Cuts[0].Cut, s.localCut)
			s.localCutMu.Unlock()

			if measureOrderingInterval {
				for _, c := range lcs.Cuts[0].Cut {
					if c > 0 {
						for j := s.prevSentLocalCut; j < c; j++ {
							s.recordsMu.Lock()
							s.timeOfEntry[j] = time.Now()
							s.recordsMu.Unlock()
						}
						s.prevSentLocalCut = c
					}
				}
			}

			log.Debugf("records sent: %v", lcs.Cuts[0].Cut[s.replicaID]-prevCut)
			prevCut = lcs.Cuts[0].Cut[s.replicaID]

			s.localCutNum += 1
			if qcExpt {
				if qcBurstLSN != -1 && prevCut > qcBurstLSN && qcBurstLocalCutNumber == -1 {
					qcBurstLocalCutNumber = s.localCutNum
					log.Printf("burst local cut number %v", qcBurstLocalCutNumber)
				}
			}
			lcs.Cuts[0].LocalCutNum = s.localCutNum

			// log.Debugf("Data report: %v", lcs)
			err := (*s.orderClient).Send(lcs)
			if err != nil {
				log.Errorf("%v", err)
			}
		case <-s.stopReport:
			return
		}
	}
}

func (s *DataServer) receiveCommittedCut() {
	for {
		e, err := (*s.orderClient).Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Fatalf("Receive from ordering layer error: %v", err)
		}
		s.committedEntryC <- e
	}
}

func (s *DataServer) processCommittedEntry() {
	prevGCutTime := time.Now()
	for entry := range s.committedEntryC {
		if entry.CommittedCut != nil {
			log.Debugf("time since last committed cut: %v", time.Since(prevGCutTime).Nanoseconds())
			prevGCutTime = time.Now()
			startReplicaID := s.shardID * s.numReplica
			startGSN := entry.CommittedCut.StartGSN
			// compute startGSN using the number of records stored
			// in shards with smaller ids
			for rid, lsn := range entry.CommittedCut.Cut {
				if rid < startReplicaID {
					diff := lsn
					if l, ok := s.prevCommittedCut.Cut[rid]; ok {
						diff = lsn - l
					}
					if diff > 0 {
						startGSN += diff
					}
				}
			}
			// assign gsn to records in my shard
			for i := int32(0); i < s.numReplica; i++ {
				rid := startReplicaID + i
				lsn := entry.CommittedCut.Cut[rid]
				diff := int32(lsn)
				start := int64(0)
				if l, ok := s.prevCommittedCut.Cut[rid]; ok {
					start = l
					diff = int32(lsn - l)
				}
				if diff > 0 {
					err := s.storage.Assign(i, start, diff, startGSN)
					if err != nil {
						log.Errorf("Assign GSN to storage error: %v", err)
						continue
					}
					if i == s.replicaID {
						for j := int32(0); j < diff; j++ {
							s.recordsMu.Lock()
							record, ok := s.records[start+int64(j)]
							if ok {
								delete(s.records, start+int64(j))
								if measureOrderingInterval {
									elapsed := time.Since(s.timeOfEntry[start+int64(j)])
									s.avgOrderingLatencyMicros = (s.avgOrderingLatencyMicros*float64(s.numOrders) + float64(elapsed.Microseconds())) / float64(s.numOrders+1)
									s.numOrders++
									delete(s.timeOfEntry, start+int64(j))
								}
							}
							s.recordsMu.Unlock()
							if measureOrderingInterval {
								log.Debugf("avg ordering latency: %v", s.avgOrderingLatencyMicros)
							}
							if !ok {
								log.Errorf("error, not able to find records")
								continue
							}
							ack := &datapb.Ack{
								ClientID:       record.ClientID,
								ClientSN:       record.ClientSN,
								ShardID:        s.shardID,
								LocalReplicaID: s.replicaID,
								ViewID:         s.viewID,
								GlobalSN:       startGSN + int64(j),
							}
							s.ackC <- ack

							// send record on exposable records channel
							record.GlobalSN = startGSN + int64(j)
							record.LocalReplicaID = s.replicaID
							record.ShardID = s.shardID
							record.ViewID = s.viewID

							s.committedRecordsMu.Lock()
							s.committedRecords[startGSN+int64(j)] = record
							s.committedRecordsMu.Unlock()

							s.liveSubscribeC <- record.GlobalSN
						}
					}
					startGSN += int64(diff)
				}
			}
			log.Debugf("committed cut: %v", entry.CommittedCut)
			// replace previous committed cut
			s.prevCommittedCut = entry.CommittedCut
		}
		if entry.FinalizeShards != nil { //nolint
			for _, shardID := range entry.FinalizeShards.ShardIDs {
				if shardID == s.shardID {
					// stop local reporting
					s.stopReport <- true
				}
			}
		}
	}
}

func (s *DataServer) CreateAck(cid, csn int32) {
	id := int64(cid)<<32 + int64(csn)
	ackC := make(chan *datapb.Ack, 1)
	s.waitMu.Lock()
	s.wait[id] = ackC
	s.waitMu.Unlock()
}

func (s *DataServer) WaitForAck(cid, csn int32) *datapb.Ack {
	id := int64(cid)<<32 + int64(csn)
	s.waitMu.RLock()
	ackC, ok := s.wait[id]
	s.waitMu.RUnlock()
	if !ok {
		log.Errorf("error, never occurs")
	}
	ack := <-ackC
	s.waitMu.Lock()
	delete(s.wait, id)
	s.waitMu.Unlock()
	return ack
}
