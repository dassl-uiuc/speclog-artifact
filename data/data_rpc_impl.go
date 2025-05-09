package data

import (
	"context"
	"io"

	"github.com/scalog/scalog/data/datapb"
	log "github.com/scalog/scalog/logger"
)

func (s *DataServer) Append(stream datapb.Data_AppendServer) error {
	initialized := false
	done := make(chan struct{})
	for {
		select {
		case <-done:
			return nil
		default:
			record, err := stream.Recv()
			if err != nil {
				close(done)
				if err == io.EOF {
					log.Infof("Receive append stream closed.")
					return nil
				}
				return err
			}
			if !initialized {
				cid := record.ClientID
				ackSendC := make(chan *datapb.Ack, 4096)
				s.ackSendCMu.Lock()
				s.ackSendC[cid] = ackSendC
				s.ackSendCMu.Unlock()
				go s.respondToClient(cid, done, stream)
				initialized = true
			}
			s.appendC <- record
			s.recordsInSystem.Add(1)
		}
	}
}

func (s *DataServer) AppendOne(ctx context.Context, record *datapb.Record) (*datapb.Ack, error) {
	s.CreateAck(record.ClientID, record.ClientSN)
	s.appendC <- record
	s.recordsInSystem.Add(1)
	ack := s.WaitForAck(record.ClientID, record.ClientSN)
	return ack, nil
}

func (s *DataServer) respondToClient(cid int32, done chan struct{}, stream datapb.Data_AppendServer) {
	s.ackSendCMu.RLock()
	ackSendC := s.ackSendC[cid]
	s.ackSendCMu.RUnlock()
	defer func() {
		s.ackSendCMu.Lock()
		delete(s.ackSendC, cid)
		s.ackSendCMu.Unlock()
		close(ackSendC)
	}()
	for {
		select {
		case <-done:
			return
		case ack := <-ackSendC:
			if err := stream.Send(ack); err != nil {
				close(done)
				return
			}
		}
	}
}

func (s *DataServer) Replicate(stream datapb.Data_ReplicateServer) error {
	for {
		record, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				log.Infof("Receive replicate stream closed.")
				return nil
			}
			log.Errorf("Receive replicate error: %v", err)
			return err
		}
		s.diskWriteMu.Lock()
		id := int64(record.ClientID)<<32 + int64(record.ClientSN)
		c := make(chan bool)
		s.diskWriteC[id] = c
		s.diskWriteMu.Unlock()

		// this replicated record is a hole
		// only write one record for multiple holes
		s.replicateC <- record
		<-c

		ack := &datapb.Ack{
			ClientID:       record.ClientID,
			ClientSN:       record.ClientSN,
			ShardID:        s.shardID,
			LocalReplicaID: s.replicaID,
			ViewID:         s.viewID,
			GlobalSN:       0,
		}
		if err = stream.Send(ack); err != nil {
			return err
		}
		s.diskWriteMu.Lock()
		delete(s.diskWriteC, id)
		s.diskWriteMu.Unlock()
	}
}

// TODO implement the trim operation
func (s *DataServer) Trim(ctx context.Context, gsn *datapb.GlobalSN) (*datapb.Ack, error) {
	return &datapb.Ack{}, nil
}

func (s *DataServer) Read(ctx context.Context, gsn *datapb.GlobalSN) (*datapb.Record, error) {
	r, err := s.storage.Read(gsn.GSN)
	if err != nil {
		return &datapb.Record{}, nil
	}
	record := &datapb.Record{
		GlobalSN:       gsn.GSN,
		ShardID:        s.shardID,
		LocalReplicaID: 0, // TODO figure out local replica id
		ViewID:         s.viewID,
		Record:         r,
	}
	return record, nil
}

func (s *DataServer) Subscribe(gsn *datapb.GlobalSN, stream datapb.Data_SubscribeServer) error {
	subC := make(chan *datapb.Record, 4096)
	clientSub := &clientSubscriber{
		state:    BEHIND,
		respChan: subC,
		startGsn: gsn.GSN,
	}
	s.newClientSubscribersChan <- clientSub

	for sub := range subC {
		err := stream.Send(sub)
		if err == nil {
			continue
		}
		log.Debugf("Send record error: %v, closing channel...", err)
		clientSub.state = CLOSED
		close(subC)
		return err
	}

	clientSub.state = CLOSED
	return nil
}

func (s *DataServer) FilterSubscribe(gsn *datapb.FilterGlobalSN, stream datapb.Data_FilterSubscribeServer) error {
	subC := make(chan *datapb.Record, 4096)
	clientSub := &clientSubscriber{
		state:    BEHIND,
		respChan: subC,
		startGsn: gsn.GSN,
	}
	s.newClientSubscribersChan <- clientSub

	missedRecords := make([]int64, 10000000)
	numMissedRecords := 0
	for sub := range subC {
		if sub.ClientID == -1 {
			err := stream.Send(sub)
			if err == nil {
				continue
			}
			log.Debugf("Send record error: %v, closing channel...", err)
			clientSub.state = CLOSED
			close(subC)
			return err
		} else {
			if (sub.RecordID % gsn.FilterValue) == gsn.ReaderID {
				log.Debugf("Sending record %v to reader %v", sub.RecordID, gsn.ReaderID)
				sub.MissedRecords = missedRecords[:numMissedRecords]

				err := stream.Send(sub)
				if err == nil {
					numMissedRecords = 0
					continue
				}
				log.Debugf("Send record error: %v, closing channel...", err)
				clientSub.state = CLOSED
				close(subC)
				return err
			} else {
				missedRecords[numMissedRecords] = sub.GlobalSN
				numMissedRecords++
			}
		}
	}

	clientSub.state = CLOSED
	return nil
}

func (s *DataServer) FilterSubscribeDouble(gsn *datapb.FilterGlobalSN, stream datapb.Data_FilterSubscribeDoubleServer) error {
	subC := make(chan *datapb.Record, 4096)
	clientSub := &clientSubscriber{
		state:    BEHIND,
		respChan: subC,
		startGsn: gsn.GSN,
	}
	s.newClientSubscribersChan <- clientSub

	missedRecords := make([]int64, 10000000)
	numMissedRecords := 0
	// unpack read id
	// readerId := int32(uint32(gsn.ReaderID) >> 16)
	// readerId2 := int32(gsn.ReaderID & 0xFFFF)
	readerId := gsn.ReaderID
	readerId2 := gsn.ReaderID2
	log.Infof("filtering recordID %d, %d with filter value %d", readerId, readerId2, gsn.FilterValue)
	for sub := range subC {
		if sub.ClientID == -1 {
			err := stream.Send(sub)
			if err == nil {
				continue
			}
			log.Debugf("Send record error: %v, closing channel...", err)
			clientSub.state = CLOSED
			close(subC)
			return err
		} else {
			if (sub.RecordID%gsn.FilterValue) == readerId || (sub.RecordID%gsn.FilterValue) == readerId2 {
				// if (sub.RecordID % gsn.FilterValue) == readerId {
				// log.Infof("Sending record %v to reader %v", sub.RecordID, gsn.ReaderID)
				sub.MissedRecords = missedRecords[:numMissedRecords]

				err := stream.Send(sub)
				if err == nil {
					numMissedRecords = 0
					continue
				}
				log.Debugf("Send record error: %v, closing channel...", err)
				clientSub.state = CLOSED
				close(subC)
				return err
			} else {
				missedRecords[numMissedRecords] = sub.GlobalSN
				numMissedRecords++
				if numMissedRecords >= 10000000 {
					log.Warningf("missing records too much: %d", numMissedRecords)
				}
			}
		}
	}

	clientSub.state = CLOSED
	return nil
}
