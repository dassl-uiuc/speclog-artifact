package scalog_api

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/scalog/scalog/client"
	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/pkg/address"
	"github.com/spf13/viper"
	rateLimiter "golang.org/x/time/rate"
)

type BenchmarkStats struct {
	// both of these are maps from GSN to time
	DeliveryTime        map[int64]time.Time
	AppendEndTime       map[int64]time.Time
	AppendStartTime     map[int64]time.Time
	AppendStartTimeChan chan time.Time // i will get my own acks in order
}

type Scalog struct {
	client      *client.Client
	records     []client.CommittedRecord
	atomicInt   int64
	rateLimiter *rateLimiter.Limiter
	rate        int // rate used by rate limiter
	Stats       BenchmarkStats
	Stop        chan bool // stop scalog client subscribing, must be closed by app
	StopAck     chan bool // stop ack thread, must be closed by app
}

func (s *Scalog) AppendToAssignedShard(appenderId int32, record string) error {
	// first call creates rate limiter
	if s.rateLimiter == nil {
		s.rateLimiter = rateLimiter.NewLimiter(rateLimiter.Limit(s.rate), 1)
		// start ack thread
		go s.Ack()
	}

	err := s.rateLimiter.Wait(context.Background())
	if err != nil {
		return fmt.Errorf("rate limiter error: %v", err)
	}

	_, _, err = s.client.AppendToAssignedShard(appenderId, record)
	if err != nil {
		log.Errorf("%v", err)
	}

	s.Stats.AppendStartTimeChan <- time.Now()

	return err
}

func (s *Scalog) Ack() {
	for {
		select {
		case ack := <-s.client.AckC:
			startTime := <-s.Stats.AppendStartTimeChan
			s.Stats.AppendEndTime[ack.GlobalSN] = time.Now()
			s.Stats.AppendStartTime[ack.GlobalSN] = startTime
		case <-s.StopAck:
			return
		}
	}
}

func (s *Scalog) AppendOneToAssignedShard(appenderId int32, record string) int64 {
	startTime := time.Now()
	gsn, _, err := s.client.AppendOneToAssignedShard(appenderId, record)
	if err != nil {
		log.Errorf("%v", err)
	}

	s.Stats.AppendStartTime[gsn] = startTime
	s.Stats.AppendEndTime[gsn] = time.Now()

	return gsn
}

func (s *Scalog) AppendOne(record string) int64 {
	startTime := time.Now()
	gsn, _, err := s.client.AppendOne(record)
	if err != nil {
		log.Errorf("%v", err)
	}
	s.Stats.AppendStartTime[gsn] = startTime
	s.Stats.AppendEndTime[gsn] = time.Now()

	return gsn
}

func (s *Scalog) Append(record string) error {
	// first call creates rate limiter
	if s.rateLimiter == nil {
		s.rateLimiter = rateLimiter.NewLimiter(rateLimiter.Limit(s.rate), 1)
		// start ack thread
		go s.Ack()
	}

	err := s.rateLimiter.Wait(context.Background())
	if err != nil {
		return fmt.Errorf("rate limiter error: %v", err)
	}

	_, _, err = s.client.Append(record)
	if err != nil {
		log.Errorf("%v", err)
	}
	s.Stats.AppendStartTimeChan <- time.Now()

	return err
}

func (s *Scalog) SubscribeToAssignedShardThread(readerId int32, startGsn int64) {
	stream, err := s.client.SubscribeToAssignedShard(startGsn, readerId)
	if err != nil {
		log.Errorf("%v", err)
	}

	for {
		select {
		case <-s.Stop:
			return
		case r := <-stream:
			s.Stats.DeliveryTime[r.GSN] = time.Now()
			index := atomic.LoadInt64(&s.atomicInt)
			s.records[index] = r
			atomic.AddInt64(&s.atomicInt, 1)
			continue
		}
	}
}

func (s *Scalog) SubscribeToAssignedShard(readerId int32, startGsn int64) {
	go s.SubscribeToAssignedShardThread(readerId, startGsn)
}

func (s *Scalog) SubscribeThread(startGsn int64) {
	stream, err := s.client.Subscribe(startGsn)
	if err != nil {
		log.Errorf("%v", err)
	}
	prevGsn := int64(-1)

	for {
		select {
		case <-s.Stop:
			return
		case r := <-stream:
			if r.GSN != prevGsn+1 {
				log.Errorf("[scalog_api]: out of order record: %v", r.GSN)
			}
			prevGsn = r.GSN
			index := atomic.LoadInt64(&s.atomicInt)
			s.records[index] = r
			atomic.AddInt64(&s.atomicInt, 1)
			s.Stats.DeliveryTime[r.GSN] = time.Now()
			continue
		}
	}
}

func (s *Scalog) Subscribe(startGsn int64) {
	go s.SubscribeThread(startGsn)
}

func (s *Scalog) GetLatestOffset() int64 {
	// Return atomic var
	return atomic.LoadInt64(&s.atomicInt)
}

func (s *Scalog) Read(index int64) client.CommittedRecord {
	return s.records[index]
}

// rate limit: if intended to be used for append stream
// sharding hint: to be used if provided, if not -1
func CreateClient(rateLimit int, shardingHint int, configFile string) *Scalog {
	var err error

	// read configuration file
	viper.SetConfigFile(configFile)
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

	var c *client.Client
	if shardingHint == -1 {
		c, err = client.NewClient(dataAddr, discAddr, numReplica)
		if err != nil {
			log.Fatalf("%v", err)
		}
	} else {
		c, err = client.NewClientWithShardingHint(dataAddr, discAddr, numReplica, int64(shardingHint))
		if err != nil {
			log.Fatalf("%v", err)
		}
	}

	records := make([]client.CommittedRecord, 10000000)

	stats := BenchmarkStats{
		DeliveryTime:        make(map[int64]time.Time),
		AppendEndTime:       make(map[int64]time.Time),
		AppendStartTime:     make(map[int64]time.Time),
		AppendStartTimeChan: make(chan time.Time, 100), // do not need more than this
	}
	scalogClient := &Scalog{
		client:    c,
		records:   records,
		atomicInt: 0,
		rate:      rateLimit,
		Stats:     stats,
		Stop:      make(chan bool, 1),
		StopAck:   make(chan bool, 1),
	}

	return scalogClient
}