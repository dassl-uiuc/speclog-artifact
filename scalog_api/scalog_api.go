package scalog_api

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/scalog/scalog/client"
	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/pkg/address"
	"github.com/spf13/viper"
)

type Scalog struct {
	client *client.Client
	records []string
	atomicInt int64
}

func (s *Scalog) AppendToAssignedShard(appenderId int32, record string) int64 {
	gsn, _, err := s.client.AppendToAssignedShard(appenderId, record)
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

func (s *Scalog) SubscribeToAssignedShard(readerId int32) {
	go s.SubscribeToAssignedShardThread(readerId)
}

func (s *Scalog) SubscribeToAssignedShardThread(readerId int32) {
	stream, err := s.client.SubscribeToAssignedShard(0, readerId)
	if err != nil {
		log.Errorf("%v", err)
	}
	for r := range stream {
		index := atomic.LoadInt64(&s.atomicInt)

		s.records[index] = r.Record

		// Increment atomic var
		atomic.AddInt64(&s.atomicInt, 1)
	}
}

func (s *Scalog) Subscribe() {
	go s.SubscribeThread()
}

func (s *Scalog) SubscribeThread() {
	stream, err := s.client.Subscribe(0)
	if err != nil {
		log.Errorf("%v", err)
	}
	for r := range stream {
		index := atomic.LoadInt64(&s.atomicInt)

		s.records[index] = r.Record

		// Increment atomic var
		atomic.AddInt64(&s.atomicInt, 1)
	}
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
	viper.SetConfigFile("../../../.scalog.yaml")
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
	records := make([]string, 1000000)

	return &Scalog{client: c, records: records, atomicInt: 0}
}