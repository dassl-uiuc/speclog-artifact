package data

import (
	"context"
	"fmt"
	"io"
	"math/rand"
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

type Stats struct {
	numHolesGenerated int64

	waitingForNextQuotaNs int64
	numQuotas             int64

	timeToFillHolesPerLFNs int64
	lagFixes               int64

	timeToComputeCommittedCutNs int64
	numCommittedCuts            int64

	totalCutsSent int64
	numCuts       int64

	totalReplicationTime int64
	numRepl              int64

	avgLocalCutInterreportNs int64

	prevNumHoles int64
}

func (s *Stats) printStats() {
	log.Printf("num holes generated: %v [+%v]", s.numHolesGenerated, s.numHolesGenerated-s.prevNumHoles)
	s.prevNumHoles = s.numHolesGenerated
	if s.numQuotas > 0 {
		log.Printf("avg time spent waiting for next quota in ms: %v", float64(s.waitingForNextQuotaNs)/float64(s.numQuotas)/1e6)
	}
	if s.lagFixes > 0 {
		log.Printf("avg time to fill holes synchronously per lag fix: %v", float64(s.timeToFillHolesPerLFNs)/float64(s.lagFixes)/1e6)
		log.Printf("num lag fixes: %v", s.lagFixes)
	}
	if s.numCommittedCuts > 0 {
		log.Printf("avg time to compute committed cut in us: %v", float64(s.timeToComputeCommittedCutNs)/float64(s.numCommittedCuts)/1e3)
	}
	if s.numCuts > 0 {
		log.Printf("avg number of batches sent in each cut: %v", float64(s.totalCutsSent)/float64(s.numCuts))
	}
	if s.numRepl > 0 {
		log.Printf("avg replication time in ms: %v", float64(s.totalReplicationTime)/float64(s.numRepl)/1e6)
	}
	if s.numCuts > 0 {
		log.Printf("avg time between local cut reports in ms: %v", float64(s.avgLocalCutInterreportNs)/float64(s.numCuts)/1e6)
	}
}

type DataServer struct {
	// data s configurations
	shardID          int32
	replicaID        int32
	numReplica       int32
	batchingInterval time.Duration
	// s state
	clientID                int32 // incremental counter to distinguish clients
	viewID                  int32
	localCut                atomic.Int64
	prevCommittedCut        *orderpb.CommittedCut
	nextExpectedLocalCutNum int64
	nextExpectedWindowNum   int64
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
	// for hole replication optimization, marker to delete wg after these many records have used the wg
	replicateCountDown map[int64]int
	replicateConfMu    sync.RWMutex

	// written to disk
	diskWriteC  map[int64]chan bool
	diskWriteMu sync.RWMutex

	// Channel used to determine if local cut should be send
	quota                 int64
	localCutNum           int64
	numLocalCutsThreshold int64
	waitForNewQuota       chan int64
	windowNumber          int64
	quotas                map[int64](map[int32]int64) // quotas for each window

	// hole filling
	nextCSNForHole    int32
	holeID            int32 // client id to generate holes
	holeWg            sync.WaitGroup
	recordsInPipeline atomic.Int64
	fixLag            chan int64

	// stats
	stats Stats

	// to measure repl time
	replStartTime map[int64]time.Time
	replMu        sync.Mutex
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
	s.localCut.Store(0)
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
	s.stats = Stats{}
	s.holeWg = sync.WaitGroup{}
	s.replicateCountDown = make(map[int64]int)
	s.replStartTime = make(map[int64]time.Time)

	s.quota = 10
	s.localCutNum = -1
	s.numLocalCutsThreshold = 100
	s.waitForNewQuota = make(chan int64, 4096)
	s.windowNumber = -1
	s.nextExpectedLocalCutNum = 0
	s.nextExpectedWindowNum = 0
	s.nextCSNForHole = -1
	s.holeID = s.generateClientIDForHole()
	s.quotas = make(map[int64](map[int32]int64))
	s.recordsInPipeline.Store(0)
	s.fixLag = make(chan int64, 4096)

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
		if record.ClientID == s.holeID {
			s.replicateCountDown[id] = int(record.NumHoles)
		}
		s.replicateConfMu.Unlock()

		s.replMu.Lock()
		s.replStartTime[id] = time.Now()
		s.replMu.Unlock()

		for i, c := range s.replicateSendC {
			if int32(i) != s.replicaID {
				log.Debugf("Data forward to %v", i)
				c <- record
			}
		}
		// if hole, use the same record and write it many times
		if record.ClientID == s.holeID {
			for i := int32(0); i < record.NumHoles; i++ {
				s.selfReplicateC <- record
			}
		} else {
			s.selfReplicateC <- record
		}
	}
}

// processReplicate writes my peers records to local storage
func (s *DataServer) processReplicate() {
	for record := range s.replicateC {
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
		if record.ClientID == s.holeID {
			s.replicateConfMu.RLock()
			recordID := int64(record.ClientID)<<32 + int64(record.ClientSN)
			wg := s.replicateConfWg[recordID]
			count := s.replicateCountDown[recordID]
			s.replicateConfMu.RUnlock()
			wg.Wait()
			count -= 1
			s.replicateConfMu.Lock()
			if count > 0 {
				s.replicateCountDown[recordID] = count
			} else {
				s.replicateConfWg[recordID] = nil
				delete(s.replicateConfWg, recordID)
				delete(s.replicateCountDown, recordID)
			}
			s.replicateConfMu.Unlock()
		} else {
			s.replicateConfMu.RLock()
			recordID := int64(record.ClientID)<<32 + int64(record.ClientSN)
			wg := s.replicateConfWg[recordID]
			s.replicateConfMu.RUnlock()
			wg.Wait()
			s.replicateConfMu.Lock()
			s.replicateConfWg[recordID] = nil
			delete(s.replicateConfWg, recordID)
			s.replicateConfMu.Unlock()
		}

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

		s.localCut.Add(1)
		s.recordsInPipeline.Add(-1)

		if record.ClientID == s.holeID {
			s.holeWg.Done()
		}

		s.replMu.Lock()
		id := int64(record.ClientID)<<32 + int64(record.ClientSN)
		if startTime, ok := s.replStartTime[id]; ok {
			s.stats.totalReplicationTime += time.Since(startTime).Nanoseconds()
			s.stats.numRepl++
			delete(s.replStartTime, id)
		}
		s.replMu.Unlock()
	}
}

func (s *DataServer) generateClientIDForHole() int32 {
	seed := rand.NewSource(time.Now().UnixNano())
	return rand.New(seed).Int31()
}

func (s *DataServer) getNextClientSNForHole() int32 {
	s.nextCSNForHole = s.nextCSNForHole + 1
	return s.nextCSNForHole
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
		} else {
			log.Errorf("error wait does not contain clientId")
		}
	}
}

func (s *DataServer) registerToOrderingLayer() {
	orderClient := orderpb.NewOrderClient(s.orderConn)

	localCut := &orderpb.LocalCut{
		ShardID:        s.shardID,
		LocalReplicaID: s.replicaID,
		Quota:          s.quota, // Initially 4
	}
	localCut.Cut = make([]int64, s.numReplica)

	maxRetries := 5
	for i := 0; i < maxRetries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := orderClient.Register(ctx, localCut)
		if err == nil {
			fmt.Println("Successfully registered LocalCut")
			return
		}

		fmt.Printf("Register shard failed: %v\n", err)

		// Wait before retrying
		backoff := time.Duration((1 << i)) * time.Second
		fmt.Printf("Retrying in %v...\n", backoff)
		time.Sleep(backoff)
	}

	log.Errorf("Max retries reached. Registration failed.")
}

func (s *DataServer) reportLocalCut() {
	// register to ordering layer
	// TODO(shreesha00): Add timeout here and retry here
	s.registerToOrderingLayer()
	s.quota = <-s.waitForNewQuota
	s.stats.numQuotas++
	s.prevSentLocalCut = s.localCut.Load()

	if s.quota == 0 {
		log.Errorf("error: quota is 0")
	}

	// early wakeup, tick at 1/2 the interval to decide if we need holes
	tick := time.NewTicker(s.batchingInterval / 2)
	log.Printf("starting a timer for %v", s.batchingInterval/2)
	printTicker := time.NewTicker(5 * time.Second)
	var prevLocalCutTime time.Time
	tickNum := int64(0)
	var lcs *orderpb.LocalCuts
	for {
		select {
		case <-tick.C:
			tickNum++
			if tickNum%2 == 1 {
				// early wakeup, tick at 1/2 the interval to decide if we need holes

				// create lcs
				lcs = &orderpb.LocalCuts{}
				lcs.Cuts = make([]*orderpb.LocalCut, 1)
				lcs.Cuts[0] = &orderpb.LocalCut{
					ShardID:        s.shardID,
					LocalReplicaID: s.replicaID,
					WindowNum:      s.windowNumber,
				}
				lcs.Cuts[0].Cut = make([]int64, s.numReplica)
				lcs.Cuts[0].Feedback = &orderpb.Feedback{}
				lcs.Cuts[0].Cut[s.replicaID] = s.prevSentLocalCut + s.quota

				// start replication for some holes if I do not have enough records
				pipeline := s.recordsInPipeline.Load()
				currLocalCut := s.localCut.Load()
				if currLocalCut+pipeline < s.prevSentLocalCut+s.quota {
					diff := s.prevSentLocalCut + s.quota - currLocalCut - pipeline
					s.holeWg.Add(int(diff))
					holeRecord := &datapb.Record{
						ClientID: s.holeID,
						ClientSN: s.getNextClientSNForHole(),
						Record:   "",
						NumHoles: int32(diff),
					}
					s.recordsInPipeline.Add(int64(diff))
					s.appendC <- holeRecord
					s.stats.numHolesGenerated += diff
					lcs.Cuts[0].Feedback.NumHoles = diff
				}
			} else {
				// handle control singal from ordering layer to adjust lag
				select {
				case lag := <-s.fixLag:
					// need to send @lag number of local cuts, fill with holes
					log.Printf("fixing lag by sending %v local cuts", lag)

					// figure out how many entries to fill
					lcs = &orderpb.LocalCuts{}
					lcs.Cuts = make([]*orderpb.LocalCut, lag)
					numEntries := int64(0)

					for i := int64(1); i <= lag; i++ {
						s.localCutNum += 1
						lcs.Cuts[i-1] = &orderpb.LocalCut{
							ShardID:        s.shardID,
							LocalReplicaID: s.replicaID,
							WindowNum:      s.windowNumber,
							LocalCutNum:    s.localCutNum,
							Quota:          s.quota,
							Feedback:       &orderpb.Feedback{},
							// note: as a choice, we do not send any feedback to the ordering layer for these cuts as we are simply adjusting for lag, we will send feedback for the next cut
							Cut: make([]int64, s.numReplica),
						}
						numEntries += int64(s.quota)
						lcs.Cuts[i-1].Cut[s.replicaID] = s.prevSentLocalCut + numEntries

						if s.localCutNum == s.numLocalCutsThreshold-1 {
							startTime := time.Now()
							s.quota = <-s.waitForNewQuota
							s.stats.waitingForNextQuotaNs += time.Since(startTime).Nanoseconds()
							s.stats.numQuotas++
							s.windowNumber++
							s.localCutNum = -1
						}
					}

					// get current local cut
					pipeline := s.recordsInPipeline.Load()
					currLocalCut := s.localCut.Load()

					// start replication for some holes if I do not have enough records
					if currLocalCut+pipeline < s.prevSentLocalCut+int64(numEntries) {
						startTime := time.Now()
						diff := s.prevSentLocalCut + int64(numEntries) - currLocalCut - pipeline
						s.holeWg.Add(int(diff))
						holeRecord := &datapb.Record{
							ClientID: s.holeID,
							ClientSN: s.getNextClientSNForHole(),
							Record:   "",
							NumHoles: int32(diff),
						}
						s.recordsInPipeline.Add(int64(diff))
						s.appendC <- holeRecord
						s.stats.numHolesGenerated += diff
						s.holeWg.Wait()
						s.stats.timeToFillHolesPerLFNs += time.Since(startTime).Nanoseconds()
						s.stats.lagFixes += 1
					} else {
						once := false
						for s.localCut.Load() < s.prevSentLocalCut+int64(numEntries) {
							// wait until I get enough records
							// very very unlikely to happen
							if !once {
								log.Printf("holy moly, how do I have so many records ready?")
								once = true
							}
						}
					}

					s.stats.totalCutsSent += lag
					s.stats.numCuts += 1

					log.Debugf("Data report: %v", lcs)
					err := (*s.orderClient).Send(lcs)
					if prevLocalCutTime.IsZero() {
						prevLocalCutTime = time.Now()
					} else {
						s.stats.avgLocalCutInterreportNs += time.Since(prevLocalCutTime).Nanoseconds()
						prevLocalCutTime = time.Now()
					}

					if err != nil {
						log.Errorf("%v", err)
					}

					// update prevSentLocalCut
					s.prevSentLocalCut += numEntries

					// continue to the next iteration
					continue
				default:
					break
				}

				// regular case.
				// I have already replicated holes if needed, simply wait
				for s.localCut.Load() < s.prevSentLocalCut+s.quota {
					// wait until I get enough records
				}

				currLocalCut := s.localCut.Load()

				s.stats.totalCutsSent += 1
				s.stats.numCuts += 1

				lcs.Cuts[0].Cut[s.replicaID] = s.prevSentLocalCut + s.quota

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

				s.localCutNum += 1
				lcs.Cuts[0].LocalCutNum = s.localCutNum
				lcs.Cuts[0].Quota = s.quota
				lcs.Cuts[0].Feedback = &orderpb.Feedback{}
				// TODO: maybe only do this if no holes have been filled in the early wakeup stage?
				lcs.Cuts[0].Feedback.QueueLength = currLocalCut - s.prevSentLocalCut - s.quota

				log.Debugf("Data report: %v", lcs)
				err := (*s.orderClient).Send(lcs)
				if prevLocalCutTime.IsZero() {
					prevLocalCutTime = time.Now()
				} else {
					s.stats.avgLocalCutInterreportNs += time.Since(prevLocalCutTime).Nanoseconds()
					prevLocalCutTime = time.Now()
				}

				if err != nil {
					log.Errorf("%v", err)
				}

				// update prevSentLocalCut
				s.prevSentLocalCut += s.quota

				if s.localCutNum == s.numLocalCutsThreshold-1 {
					startTime := time.Now()
					s.quota = <-s.waitForNewQuota
					s.stats.waitingForNextQuotaNs += time.Since(startTime).Nanoseconds()
					s.stats.numQuotas++
					s.windowNumber++
					s.localCutNum = -1
				}
			}
		case <-printTicker.C:
			s.stats.printStats()
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
	for entry := range s.committedEntryC {
		if entry.CommittedCut != nil {
			log.Debugf("Processing committed cut: %v", entry.CommittedCut)
			startTime := time.Now()
			// update quota if new quota is received
			rid := s.shardID*s.numReplica + s.replicaID
			if entry.CommittedCut.IsShardQuotaUpdated {
				_, quotaExists := s.quotas[entry.CommittedCut.WindowNum]
				if !quotaExists {
					s.quotas[entry.CommittedCut.WindowNum] = entry.CommittedCut.ShardQuotas
				}
				if quota, ok := entry.CommittedCut.ShardQuotas[rid]; ok {
					s.waitForNewQuota <- quota
					if s.windowNumber == -1 {
						s.windowNumber = entry.CommittedCut.WindowNum
					}
				}
			}

			if entry.CommittedCut.AdjustmentSignal != nil && entry.CommittedCut.AdjustmentSignal.Lag[rid] > 0 {
				s.fixLag <- entry.CommittedCut.AdjustmentSignal.Lag[rid]
			}

			startReplicaID := s.shardID * s.numReplica
			startGSN := entry.CommittedCut.StartGSN
			for {
				log.Debugf("processing next expected local cut num: %v, next expected window num: %v", s.nextExpectedLocalCutNum, s.nextExpectedWindowNum)
				log.Debugf("entry.CommittedCut.Cut: %v", entry.CommittedCut.Cut)
				log.Debugf("prevCommittedCut.Cut: %v", s.prevCommittedCut.Cut)
				terminate := true
				for rid, lsn := range entry.CommittedCut.Cut {
					terminate = terminate && (s.prevCommittedCut.Cut[rid] == lsn)
				}
				if terminate {
					break
				}
				// compute startGSN using the number of records stored
				// in shards with smaller ids
				for rid, lsn := range entry.CommittedCut.Cut {
					if rid < startReplicaID {
						diff := lsn
						if l, ok := s.prevCommittedCut.Cut[rid]; ok {
							diff = lsn - l
						}
						if diff > 0 {
							startGSN += s.quotas[s.nextExpectedWindowNum][rid]
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
						diff := int32(s.quotas[s.nextExpectedWindowNum][rid])
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
									log.Printf("avg ordering latency: %v", s.avgOrderingLatencyMicros)
								}
								if !ok {
									log.Errorf("error, not able to find records")
									continue
								}

								// reply back to clients only for non-holes
								if record.ClientID != s.holeID {
									ack := &datapb.Ack{
										ClientID:       record.ClientID,
										ClientSN:       record.ClientSN,
										ShardID:        s.shardID,
										LocalReplicaID: s.replicaID,
										ViewID:         s.viewID,
										GlobalSN:       startGSN + int64(j),
									}
									s.ackC <- ack
								}

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

				// update previous committed cut's cut portion
				for rid := range entry.CommittedCut.Cut {
					if s.prevCommittedCut.Cut == nil {
						s.prevCommittedCut.Cut = make(map[int32]int64)
					}
					s.prevCommittedCut.Cut[rid] += s.quotas[s.nextExpectedWindowNum][rid]
				}

				// update next expected local cut number and window
				s.nextExpectedLocalCutNum = s.nextExpectedLocalCutNum + 1
				if s.nextExpectedLocalCutNum == s.numLocalCutsThreshold {
					s.nextExpectedLocalCutNum = 0
					s.nextExpectedWindowNum = s.nextExpectedWindowNum + 1
				}
			}
			// replace previous committed cut
			s.prevCommittedCut = entry.CommittedCut

			s.stats.timeToComputeCommittedCutNs += time.Since(startTime).Nanoseconds()
			s.stats.numCommittedCuts++
		}
		if entry.FinalizeShards != nil { //nolint
			// TODO
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
