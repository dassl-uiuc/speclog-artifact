package scalog_api

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/scalog/scalog/client"
	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/pkg/address"
	"github.com/spf13/viper"
)

type Scalog struct {
	client *client.Client
	records []string
	atomicInt int64
	stop chan bool
}

func (s *Scalog) AppendToAssignedShard(appenderId int32, record string) int64 {
	gsn, _, err := s.client.AppendToAssignedShard(appenderId, record)
	if err != nil {
		log.Errorf("%v", err)
	}

	return gsn
}

func (s *Scalog) Ack(stop chan bool) {
	for {
		select {
		case <-s.client.AckC:
			// Do nothing for now
		case <-stop:
			return
		}
	}
}

func (s *Scalog) AppendOneToAssignedShard(appenderId int32, record string) int64 {
	gsn, _, err := s.client.AppendOneToAssignedShard(appenderId, record)
	if err != nil {
		log.Errorf("%v", err)
	}

	return gsn
}

func (s *Scalog) Append(record string) int64 {
	gsn, _, err := s.client.AppendOne(record)
	if err != nil {
		log.Errorf("%v", err)
	}

	return gsn
}

func (s *Scalog) ConfirmationThread(conf chan client.SpeculationConf) {
	printTicker := time.NewTicker(1 * time.Second)
	confirmed := 0
	for {
		select {
		case c := <-conf:
			// tconf := time.Now()
			for i := c.StartGSN; i <= c.EndGSN; i++ {
				// timeConfirm[i] = tconf
				confirmed++
			}
			continue
		case <-printTicker.C:
			log.Printf("[intrusion detection]: confirmed %v records", confirmed)
		case <-s.stop:
			return
		}
	}
}

func (s *Scalog) SubscribeToAssignedShardThread(readerId int32) {
	stream, conf, err := s.client.SubscribeToAssignedShard(0, readerId)
	if err != nil {
		log.Errorf("%v", err)
	}
	s.stop = make(chan bool)
	go s.ConfirmationThread(conf)
	defer close(s.stop)
	// prevGsn := int64(-1)

	for r := range stream {
		// if r.GSN != prevGsn+1 {
		// 	log.Errorf("[single_client_e2e]: out of order record: %v", r.GSN)
		// }
		// prevGsn = r.GSN
		if r.Record != "0xDEADBEEF" {
			index := atomic.LoadInt64(&s.atomicInt)

			s.records[index] = r.Record

			// Increment atomic var
			atomic.AddInt64(&s.atomicInt, 1)
		}
		continue
	}
}

func (s *Scalog) SubscribeToAssignedShard(readerId int32) {
	go s.SubscribeToAssignedShardThread(readerId)
}

func (s *Scalog) SubscribeThread() {
	stream, conf, err := s.client.Subscribe(0)
	if err != nil {
		log.Errorf("%v", err)
	}
	s.stop = make(chan bool)
	go s.ConfirmationThread(conf)
	defer close(s.stop)
	prevGsn := int64(-1)

	for r := range stream {
		if r.GSN != prevGsn+1 {
			log.Errorf("[single_client_e2e]: out of order record: %v", r.GSN)
		}
		prevGsn = r.GSN
		if r.Record != "0xDEADBEEF" {
			index := atomic.LoadInt64(&s.atomicInt)

			s.records[index] = r.Record

			// Increment atomic var
			atomic.AddInt64(&s.atomicInt, 1)
		}
		continue
	}
}

func (s *Scalog) Subscribe() {
	go s.SubscribeThread()
}

func (s *Scalog) GetLatestOffset() int64 {
	// Return atomic var
	return atomic.LoadInt64(&s.atomicInt)
}

func (s *Scalog) Read(index int64) string {
	return s.records[index]
}

func CreateClient() *Scalog {
	var err error

	// read configuration file
	viper.SetConfigFile("/proj/rasl-PG0/tshong/speclog/.scalog.yaml")
	viper.AutomaticEnv()
	err = viper.ReadInConfig()
	if err != nil {
		log.Errorf("read config file error: %v", err)
	}

	numReplica := int32(viper.GetInt("data-replication-factor"))
	discPort := uint16(viper.GetInt("disc-port"))
	discIp := viper.GetString(fmt.Sprintf("disc-ip"))
	discAddr := address.NewGeneralDiscAddr(discIp, discPort)
	dataPort := uint16(viper.GetInt("data-port"))
	dataAddr := address.NewGeneralDataAddr("data-%v-%v-ip", numReplica, dataPort)

	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
	}

	c, err := client.NewClient(dataAddr, discAddr, numReplica)
	if err != nil {
		log.Fatalf("%v", err)
	}

	// Hack: array will never be moved in memory
	records := make([]string, 10000000)

	return &Scalog{client: c, records: records, atomicInt: 0}
}