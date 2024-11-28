package client

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scalog/scalog/data/datapb"
	"github.com/scalog/scalog/discovery/discpb"
	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/pkg/address"
	"github.com/scalog/scalog/pkg/view"

	"google.golang.org/grpc"
)

type ShardingPolicy interface {
	Shard(view *view.View, record string) (int32, int32)
	AssignSpecificShard(view *view.View, record string, appenderId int32) (int32, int32)
	GetShardID() int32
	GetReplicaID() int32
}

// ShardingPolicy determines which records are appended to which shards.

type tuple struct {
	ack *datapb.Ack
	err error
}

type SpeculationConf struct {
	StartGSN int64
	EndGSN   int64
}

type CommittedRecord struct {
	GSN    int64
	Record string
}

type Client struct {
	clientID           int32
	numReplica         int32
	nextCSN            int32
	nextGSN            int64
	viewID             int32
	view               *view.View
	viewC              chan *discpb.View
	appendC            chan *datapb.Record
	assignedAppendC    chan *datapb.Record
	AckC               chan *datapb.Ack
	subC               chan CommittedRecord
	confC              chan SpeculationConf
	committedRecords   map[int64]CommittedRecord
	committedRecordsMu sync.RWMutex
	nextConf           int64
	speculationConfs   map[int64]*datapb.Record
	shardingPolicy     ShardingPolicy
	shardingHint       int64 // client number, which is used to specifically connect to a replica of a shard

	discAddr   address.DiscAddr
	discConn   *grpc.ClientConn
	discClient *discpb.Discovery_DiscoverClient
	discMu     sync.Mutex

	dataAddr           address.DataAddr
	dataConn           map[int32]*grpc.ClientConn
	dataConnMu         sync.Mutex
	dataAppendClient   map[int32]datapb.Data_AppendClient
	dataAppendClientMu sync.Mutex

	outstandingRequestsLimit int32
	outstandingRequestsChan  chan bool

	shardsSubscribedTo map[int32]bool
	isReader           bool
}

func NewClientForBurst(dataAddr address.DataAddr, discAddr address.DiscAddr, numReplica int32, shardingHint int64, burstSize int32) (*Client, error) {
	c := &Client{
		clientID:     generateClientID(),
		numReplica:   numReplica,
		nextCSN:      -1,
		nextGSN:      0,
		viewID:       0,
		dataAddr:     dataAddr,
		discAddr:     discAddr,
		shardingHint: shardingHint,
	}
	c.outstandingRequestsLimit = 2 * burstSize
	c.outstandingRequestsChan = make(chan bool, c.outstandingRequestsLimit)
	c.shardingPolicy = NewShardingPolicyWithHint(numReplica, shardingHint)
	c.viewC = make(chan *discpb.View, 4096)
	c.appendC = make(chan *datapb.Record, 4096)
	c.assignedAppendC = make(chan *datapb.Record, c.outstandingRequestsLimit)
	c.AckC = make(chan *datapb.Ack, 4096)
	c.subC = make(chan CommittedRecord, 4096)
	c.dataConn = make(map[int32]*grpc.ClientConn)
	c.dataAppendClient = make(map[int32]datapb.Data_AppendClient)
	c.view = view.NewView()
	c.committedRecords = make(map[int64]CommittedRecord)
	c.speculationConfs = make(map[int64]*datapb.Record)
	c.confC = make(chan SpeculationConf, 4096)
	c.shardsSubscribedTo = make(map[int32]bool)
	c.isReader = false
	err := c.UpdateDiscovery()
	if err != nil {
		return nil, err
	}
	go c.subscribeView()
	c.Start()
	return c, nil
}

func NewClientWithShardingHint(dataAddr address.DataAddr, discAddr address.DiscAddr, numReplica int32, shardingHint int64) (*Client, error) {
	c := &Client{
		clientID:     generateClientID(),
		numReplica:   numReplica,
		nextCSN:      -1,
		nextGSN:      0,
		viewID:       0,
		dataAddr:     dataAddr,
		discAddr:     discAddr,
		shardingHint: shardingHint,
	}
	c.outstandingRequestsLimit = 5
	c.outstandingRequestsChan = make(chan bool, c.outstandingRequestsLimit)
	c.shardingPolicy = NewShardingPolicyWithHint(numReplica, shardingHint)
	c.viewC = make(chan *discpb.View, 4096)
	c.appendC = make(chan *datapb.Record, 4096)
	c.assignedAppendC = make(chan *datapb.Record, c.outstandingRequestsLimit)
	c.AckC = make(chan *datapb.Ack, 4096)
	c.subC = make(chan CommittedRecord, 4096)
	c.dataConn = make(map[int32]*grpc.ClientConn)
	c.dataAppendClient = make(map[int32]datapb.Data_AppendClient)
	c.view = view.NewView()
	c.committedRecords = make(map[int64]CommittedRecord)
	c.speculationConfs = make(map[int64]*datapb.Record)
	c.confC = make(chan SpeculationConf, 4096)
	c.shardsSubscribedTo = make(map[int32]bool)
	c.isReader = false
	err := c.UpdateDiscovery()
	if err != nil {
		return nil, err
	}
	go c.subscribeView()
	c.Start()
	return c, nil
}

func NewClient(dataAddr address.DataAddr, discAddr address.DiscAddr, numReplica int32) (*Client, error) {
	c := &Client{
		clientID:   generateClientID(),
		numReplica: numReplica,
		nextCSN:    -1,
		nextGSN:    0,
		viewID:     0,
		dataAddr:   dataAddr,
		discAddr:   discAddr,
	}
	c.outstandingRequestsLimit = 5
	c.outstandingRequestsChan = make(chan bool, c.outstandingRequestsLimit)
	c.shardingPolicy = NewDefaultShardingPolicy(numReplica)
	c.viewC = make(chan *discpb.View, 4096)
	c.appendC = make(chan *datapb.Record, 4096)
	c.assignedAppendC = make(chan *datapb.Record, c.outstandingRequestsLimit)
	c.AckC = make(chan *datapb.Ack, 4096)
	c.subC = make(chan CommittedRecord, 4096)
	c.dataConn = make(map[int32]*grpc.ClientConn)
	c.dataAppendClient = make(map[int32]datapb.Data_AppendClient)
	c.view = view.NewView()
	c.committedRecords = make(map[int64]CommittedRecord)
	c.speculationConfs = make(map[int64]*datapb.Record)
	c.confC = make(chan SpeculationConf, 4096)
	c.shardsSubscribedTo = make(map[int32]bool)
	c.isReader = false
	err := c.UpdateDiscovery()
	if err != nil {
		return nil, err
	}
	go c.subscribeView()
	c.Start()
	return c, nil
}

func generateClientID() int32 {
	seed := rand.NewSource(time.Now().UnixNano())
	return rand.New(seed).Int31()
}

func (c *Client) subscribeView() {
	for {
		v, err := (*c.discClient).Recv()
		if err != nil {
			log.Errorf("%v", err)
			return
		}
		log.Infof("View id: %v", v.ViewID)
		log.Infof("Live shards: %v", v.LiveShards)
		log.Infof("Finalized shards: %v", v.FinalizedShards)
		err = c.view.Update(v)
		if err != nil {
			log.Errorf("%v", err)
		}

		if c.isReader {
			for _, shard := range v.LiveShards {
				if _, ok := c.shardsSubscribedTo[shard]; !ok {
					for replicaId := int32(0); replicaId < c.numReplica; replicaId++ {
						go c.subscribeShardServer(shard, replicaId)
					}
					log.Printf("Subscribed to shard %v", shard)
					c.shardsSubscribedTo[shard] = true
				}
			}
		}
	}
}

func (c *Client) UpdateDiscovery() error {
	return c.UpdateDiscoveryAddr(c.discAddr.Get())
}

func (c *Client) UpdateDiscoveryAddr(addr string) error {
	c.discMu.Lock()
	defer c.discMu.Unlock()
	if c.discConn != nil {
		c.discConn.Close()
		c.discConn = nil
	}
	opts := []grpc.DialOption{grpc.WithInsecure(), grpc.WithBlock()}
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return fmt.Errorf("Dial discovery at %v failed: %v", addr, err)
	}
	c.discConn = conn
	discClient := discpb.NewDiscoveryClient(conn)
	callOpts := []grpc.CallOption{}
	discDiscoveryClient, err := discClient.Discover(context.Background(), &discpb.Empty{}, callOpts...)
	if err != nil {
		return fmt.Errorf("Create replicate client to %v failed: %v", addr, err)
	}
	c.discClient = &discDiscoveryClient

	v, err := (*c.discClient).Recv()
	if err != nil {
		log.Errorf("%v", err)
	}
	// log.Debugf("Discovery updating view: %v", v)
	err = c.view.Update(v)
	if err != nil {
		log.Errorf("%v", err)
	}

	return nil
}

// the caller is responsible to lock the data
func (c *Client) connDataServer(shard, replica int32) (*grpc.ClientConn, error) {
	globalReplicaID := shard*c.numReplica + replica
	addr := c.dataAddr.Get(shard, replica)
	if conn, ok := c.dataConn[globalReplicaID]; ok && conn != nil {
		c.dataConn[globalReplicaID].Close()
		delete(c.dataConn, globalReplicaID)
	}
	opts := []grpc.DialOption{grpc.WithInsecure(), grpc.WithBlock()}
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("Dial data server at %v failed: %v", addr, err)
	}
	c.dataConn[globalReplicaID] = conn
	return conn, nil
}

func (c *Client) getDataAppendClient(shard, replica int32) (datapb.Data_AppendClient, error) {
	globalReplicaID := shard*c.numReplica + replica
	c.dataAppendClientMu.Lock()
	defer c.dataAppendClientMu.Unlock()
	if client, ok := c.dataAppendClient[globalReplicaID]; ok && client != nil {
		return client, nil
	}
	client, err := c.buildDataAppendClient(shard, replica)
	if err != nil {
		return nil, err
	}

	go c.processAck(&client)

	return client, err
}

func (c *Client) buildDataAppendClient(shard, replica int32) (datapb.Data_AppendClient, error) {
	globalReplicaID := shard*c.numReplica + replica
	conn, err := c.getDataServerConn(shard, replica)
	if err != nil {
		return nil, err
	}
	dataClient := datapb.NewDataClient(conn)
	dataAppendClient, err := dataClient.Append(context.Background())
	if err != nil {
		return nil, fmt.Errorf("Build data append client of shard %v replica %v failed: %v", shard, replica, err)
	}
	c.dataAppendClient[globalReplicaID] = dataAppendClient
	return dataAppendClient, nil
}

func (c *Client) getDataServerConn(shard, replica int32) (*grpc.ClientConn, error) {
	globalReplicaID := shard*c.numReplica + replica
	c.dataConnMu.Lock()
	defer c.dataConnMu.Unlock()
	if conn, ok := c.dataConn[globalReplicaID]; ok && conn != nil {
		return conn, nil
	}
	return c.connDataServer(shard, replica)
}

func (c *Client) Start() {
	go c.processView()
	go c.processAppend()
	go c.processAssignedAppend()
	// go c.processAck()
}

func (c *Client) processView() {
	for v := range c.viewC {
		log.Debugf("Client: %v", v)
		err := c.view.Update(v)
		if err != nil {
			log.Errorf("%v", err)
		}
	}
}

func (c *Client) processAppend() {
	for r := range c.appendC {
		shard, replica := c.shardingPolicy.Shard(c.view, r.Record)
		// log.Infof("shard: %v, replica: %v\n", shard, replica)
		client, err := c.getDataAppendClient(shard, replica)
		if err != nil {
			log.Errorf("%v", err)
			return
		}
		err = client.Send(r)
		if err != nil {
			log.Errorf("%v", err)
			return
		}
	}
}

func (c *Client) processAck(client *datapb.Data_AppendClient) {
	for {
		ack, err := (*client).Recv()
		if err != nil {
			if err == io.EOF {
				log.Infof("Stream closed by server.")
				return
			}
			log.Errorf("Failed to receive ack: %v", err)
			return
		}

		<-c.outstandingRequestsChan
		c.AckC <- ack
	}
}

func (c *Client) getNextClientSN() int32 {
	csn := atomic.AddInt32(&c.nextCSN, 1)
	return csn
}

func (c *Client) Append(record string) (int64, int32, error) {
	select {
	case c.outstandingRequestsChan <- true:
	case <-time.After(1 * time.Second):
		log.Printf("Timeout waiting for outstanding requests")
		return 0, 0, fmt.Errorf("Timeout waiting for outstanding requests")
	}
	r := &datapb.Record{
		ClientID: c.clientID,
		ClientSN: c.getNextClientSN(),
		Record:   record,
	}
	c.appendC <- r
	return 0, 0, nil
}

func (c *Client) FilterAppend(record string, recordId int32) (int64, int32, error) {
	select {
	case c.outstandingRequestsChan <- true:
	case <-time.After(1 * time.Second):
		log.Printf("Timeout waiting for outstanding requests")
		return 0, 0, fmt.Errorf("Timeout waiting for outstanding requests")
	}
	r := &datapb.Record{
		ClientID: c.clientID,
		ClientSN: c.getNextClientSN(),
		Record:   record,
		RecordID: recordId,
	}
	c.appendC <- r
	return 0, 0, nil
}

func (c *Client) AppendOneTimeout(record string, timeout time.Duration) (int64, int32, error) {
	r := &datapb.Record{
		ClientID: c.clientID,
		ClientSN: c.getNextClientSN(),
		Record:   record,
	}
	shard, replica := c.shardingPolicy.Shard(c.view, record)
	conn, err := c.getDataServerConn(shard, replica)
	if err != nil {
		return 0, 0, err
	}

	// Create a context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	opts := []grpc.CallOption{}
	dataClient := datapb.NewDataClient(conn)
	ack, err := dataClient.AppendOne(ctx, r, opts...)
	if err != nil {
		return 0, 0, err
	}
	return ack.GlobalSN, ack.ShardID, nil
}

func (c *Client) AppendOne(record string) (int64, int32, error) {
	r := &datapb.Record{
		ClientID: c.clientID,
		ClientSN: c.getNextClientSN(),
		Record:   record,
	}
	shard, replica := c.shardingPolicy.Shard(c.view, record)
	// log.Infof("shard: %v, replica: %v\n", shard, replica)
	conn, err := c.getDataServerConn(shard, replica)
	if err != nil {
		return 0, 0, err
	}
	opts := []grpc.CallOption{}
	dataClient := datapb.NewDataClient(conn)
	ack, err := dataClient.AppendOne(context.TODO(), r, opts...)
	if err != nil {
		return 0, 0, err
	}
	return ack.GlobalSN, ack.ShardID, nil
}

func (c *Client) FilterAppendOne(record string, recordId int32) (int64, int32, error) {
	r := &datapb.Record{
		ClientID: c.clientID,
		ClientSN: c.getNextClientSN(),
		Record:   record,
		RecordID: recordId,
	}
	shard, replica := c.shardingPolicy.Shard(c.view, record)
	// log.Infof("shard: %v, replica: %v\n", shard, replica)
	conn, err := c.getDataServerConn(shard, replica)
	if err != nil {
		return 0, 0, err
	}
	opts := []grpc.CallOption{}
	dataClient := datapb.NewDataClient(conn)
	ack, err := dataClient.AppendOne(context.TODO(), r, opts...)
	if err != nil {
		return 0, 0, err
	}
	return ack.GlobalSN, ack.ShardID, nil
}

// hacky function just designed for the reconfiguration experiment
// this is needed to ensure that the newly spawned clients are able to append to the correct shard
func (c *Client) WaitForLiveShardSize(size int) {
	for len(c.view.LiveShards) < int(size) {
		// busy wait, FIWB
	}
}

func (c *Client) ShardLeft(shardId int32) bool {
	for _, shard := range c.view.LiveShards {
		if shard == shardId {
			return false
		}
	}
	return true
}

func (c *Client) processAssignedAppend() {
	for r := range c.assignedAppendC {
		shard, replica := c.shardingPolicy.AssignSpecificShard(c.view, r.Record, r.AppenderID)
		// log.Infof("shard: %v, replica: %v\n", shard, replica)
		client, err := c.getDataAppendClient(shard, replica)
		if err != nil {
			log.Errorf("%v", err)
			return
		}
		err = client.Send(r)
		if err != nil {
			log.Errorf("%v", err)
			return
		}
	}
}

func (c *Client) AppendToAssignedShard(appenderId int32, record string) (int64, int32, error) {
	c.outstandingRequestsChan <- true
	r := &datapb.Record{
		ClientID:   c.clientID,
		ClientSN:   c.getNextClientSN(),
		Record:     record,
		AppenderID: appenderId,
	}
	c.assignedAppendC <- r
	return 0, 0, nil
}

func (c *Client) AppendOneToAssignedShard(appenderId int32, record string) (int64, int32, error) {
	r := &datapb.Record{
		ClientID: c.clientID,
		ClientSN: c.getNextClientSN(),
		Record:   record,
	}

	shard, replica := c.shardingPolicy.AssignSpecificShard(c.view, record, appenderId)
	conn, err := c.getDataServerConn(shard, replica)
	if err != nil {
		return 0, 0, err
	}
	opts := []grpc.CallOption{}
	dataClient := datapb.NewDataClient(conn)
	ack, err := dataClient.AppendOne(context.TODO(), r, opts...)
	if err != nil {
		return 0, 0, err
	}
	return ack.GlobalSN, ack.ShardID, nil
}

func (c *Client) Read(gsn int64, shard, replica int32) (string, error) {
	globalSN := &datapb.GlobalSN{GSN: gsn}
	conn, err := c.getDataServerConn(shard, replica)
	if err != nil {
		return "", err
	}
	opts := []grpc.CallOption{}
	dataClient := datapb.NewDataClient(conn)
	record, err := dataClient.Read(context.TODO(), globalSN, opts...)
	if err != nil {
		return "", err
	}
	return record.Record, nil
}

func (c *Client) Subscribe(gsn int64) (chan CommittedRecord, chan SpeculationConf, error) {
	c.committedRecordsMu.Lock()
	c.nextGSN = gsn
	c.nextConf = gsn
	c.committedRecordsMu.Unlock()

	for _, shard := range c.view.LiveShards {
		for replicaId := int32(0); replicaId < c.numReplica; replicaId++ {
			go c.subscribeShardServer(shard, replicaId)
		}

		c.shardsSubscribedTo[shard] = true
	}
	c.isReader = true

	return c.subC, c.confC, nil
}

func (c *Client) subscribeShardServer(shard, replica int32) {
	conn, err := c.getDataServerConn(shard, replica)
	if err != nil {
		log.Errorf("%v", err)
		return
	}
	opts := []grpc.CallOption{}
	dataClient := datapb.NewDataClient(conn)
	globalSN := &datapb.GlobalSN{GSN: c.nextGSN}
	stream, err := dataClient.Subscribe(context.Background(), globalSN, opts...)
	if err != nil {
		log.Errorf("%v", err)
		return
	}
	for {
		record, err := stream.Recv()
		if err == io.EOF {
			log.Infof("Receive subscribe stream closed.")
			return
		}
		if err != nil {
			log.Errorf("%v", err)
			return
		}

		c.committedRecordsMu.Lock()
		if record.ClientID != -1 {
			c.committedRecords[record.GlobalSN] = CommittedRecord{
				GSN:    record.GlobalSN,
				Record: record.Record,
			}
			if record.GlobalSN == c.nextGSN {
				c.respondToClient()
			}
		} else {
			// this is a speculation confirmation
			c.speculationConfs[record.GlobalSN] = record
			if record.GlobalSN == c.nextConf {
				c.confirmToClient()
			}
		}
		c.committedRecordsMu.Unlock()
		// TODO(shreesha): handle view change
	}
}

func (c *Client) FilterSubscribe(gsn int64, readerId int32, filterValue int32) (chan CommittedRecord, chan SpeculationConf, error) {
	c.committedRecordsMu.Lock()
	c.nextGSN = gsn
	c.nextConf = gsn
	c.committedRecordsMu.Unlock()

	for _, shard := range c.view.LiveShards {
		for replicaId := int32(0); replicaId < c.numReplica; replicaId++ {
			go c.filterSubscribeShardServer(shard, replicaId, readerId, filterValue)
		}

		c.shardsSubscribedTo[shard] = true
	}
	c.isReader = true

	return c.subC, c.confC, nil
}

func (c *Client) filterSubscribeShardServer(shard, replica int32, readerId int32, filterValue int32) {
	conn, err := c.getDataServerConn(shard, replica)
	if err != nil {
		log.Errorf("%v", err)
		return
	}
	opts := []grpc.CallOption{}
	dataClient := datapb.NewDataClient(conn)
	globalSN := &datapb.FilterGlobalSN{GSN: c.nextGSN, ReaderID: readerId, FilterValue: filterValue}
	stream, err := dataClient.FilterSubscribe(context.Background(), globalSN, opts...)
	if err != nil {
		log.Errorf("%v", err)
		return
	}
	for {
		record, err := stream.Recv()
		if err == io.EOF {
			log.Infof("Receive subscribe stream closed.")
			return
		}
		if err != nil {
			log.Errorf("%v", err)
			return
		}
		c.committedRecordsMu.Lock()
		if record.ClientID != -1 {
			for _, gsn := range record.MissedRecords {
				c.committedRecords[gsn] = CommittedRecord{
					GSN:    gsn,
					Record: "",
				}
				if gsn == c.nextGSN {
					c.respondToClient()
				}
			}

			c.committedRecords[record.GlobalSN] = CommittedRecord{
				GSN:    record.GlobalSN,
				Record: record.Record,
			}
			if record.GlobalSN == c.nextGSN {
				c.respondToClient()
			}
		} else {
			// this is a speculation confirmation
			c.speculationConfs[record.GlobalSN] = record
			if record.GlobalSN == c.nextConf {
				c.confirmToClient()
			}
		}

		c.committedRecordsMu.Unlock()
		// TODO(shreesha): handle view change
	}
}

func (c *Client) SubscribeToAssignedShard(gsn int64, readerId int32) (chan CommittedRecord, chan SpeculationConf, error) {
	c.committedRecordsMu.Lock()
	c.nextGSN = gsn
	c.nextConf = gsn
	c.committedRecordsMu.Unlock()

	if readerId >= c.numReplica*int32(len(c.view.LiveShards)) {
		return nil, nil, fmt.Errorf("readerId %v is out of range", readerId)
	}

	shard := readerId / c.numReplica
	replica := readerId % c.numReplica

	c.shardsSubscribedTo[shard] = true
	c.isReader = true

	log.Infof("Subscribe to assigned shard %v replica %v", shard, replica)

	go c.subscribeToAssignedShardServer(shard, replica)

	return c.subC, c.confC, nil
}

func (c *Client) subscribeToAssignedShardServer(shard, replica int32) {
	conn, err := c.getDataServerConn(shard, replica)
	if err != nil {
		log.Errorf("%v", err)
		return
	}
	opts := []grpc.CallOption{}
	dataClient := datapb.NewDataClient(conn)
	globalSN := &datapb.GlobalSN{GSN: c.nextGSN}
	stream, err := dataClient.Subscribe(context.Background(), globalSN, opts...)
	if err != nil {
		log.Errorf("%v", err)
		return
	}
	for {
		record, err := stream.Recv()
		if err == io.EOF {
			log.Infof("Receive subscribe stream closed.")
			return
		}
		if err != nil {
			log.Errorf("%v", err)
			return
		}

		if record.ClientID != -1 {
			commitedRecord := CommittedRecord{
				GSN:    record.GlobalSN,
				Record: record.Record,
			}
			c.subC <- commitedRecord
		} else {
			c.confC <- SpeculationConf{StartGSN: record.GlobalSN, EndGSN: record.GlobalSN1}
		}
	}
}

// called with lock held
func (c *Client) respondToClient() {
	for {
		commitedRecord, in := c.committedRecords[c.nextGSN]
		if !in {
			break
		}
		c.subC <- commitedRecord
		delete(c.committedRecords, c.nextGSN)
		c.nextGSN++
	}
}

// called with lock held
func (c *Client) confirmToClient() {
	for {
		rec, in := c.speculationConfs[c.nextConf]
		if !in {
			break
		}
		c.confC <- SpeculationConf{StartGSN: rec.GlobalSN, EndGSN: rec.GlobalSN1}
		delete(c.speculationConfs, c.nextConf)
		c.nextConf = rec.GlobalSN1 + 1
	}
}

func (c *Client) GetShardingPolicy() (int32, int32) {
	return c.shardingPolicy.GetShardID(), c.shardingPolicy.GetReplicaID()
}

func (c *Client) SetShardingPolicy(p ShardingPolicy) {
	c.shardingPolicy = p
}
