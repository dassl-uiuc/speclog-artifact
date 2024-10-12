package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/scalog/scalog/benchmark/util"
	"github.com/scalog/scalog/client"
	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/pkg/address"
	"github.com/spf13/viper"
)

const NumberOfRequest = 10
const NumberOfBytes = 100
const StreamName = "AppendBenchmark"

const readType = "subscribe"

var wg sync.WaitGroup
var timeAppend map[int64]time.Time
var timeAppendMu sync.Mutex
var timeConsume map[int64]time.Time
var e2eLatencyMicros float64
var numRecords int64

func appendThread(client *client.Client, id int) {
	// run loop for 120 secs
	defer wg.Done()
	numRecords := 0
	ticker := time.After(120 * time.Second)
	appendLatencyAvgMicros := float64(0)
	for {
		str := util.GenerateRandomString(4096)
		startTime := time.Now()
		gsn, _, err := client.AppendOne(str)
		elapsed := time.Since(startTime)
		appendLatencyAvgMicros += float64(elapsed.Microseconds())
		timeAppendMu.Lock()
		timeAppend[gsn] = startTime
		timeAppendMu.Unlock()
		if gsn%1000 == 0 {
			fmt.Println("Client ", id, " appended ", numRecords, " records")
		}
		if err != nil {
			log.Errorf("%v", err)
		}
		numRecords++
		select {
		case <-ticker:
			fmt.Println("Client ", id, " appended ", numRecords, " records", " at avg latency of ", appendLatencyAvgMicros/float64(numRecords), " microseconds")
			return
		default:
			continue
		}
	}
}

func subscribeThread(client *client.Client, id int) {
	defer wg.Done()
	stream, err := client.Subscribe(0)
	ticker := time.After(120 * time.Second)
	if err != nil {
		log.Errorf("%v", err)
	}
	consumed := 0
	prevGsn := int64(-1)
	for {
		select {
		case r := <-stream:
			if r.GSN != prevGsn+1 {
				log.Errorf("Out of order record: %v", r.GSN)
			}
			prevGsn = r.GSN
			timeConsume[r.GSN] = time.Now()
			consumed++
			continue
		case <-ticker:
			fmt.Println("Consumed ", consumed, " records")
			return
		}
	}
}

func main() {
	timeAppend = make(map[int64]time.Time)
	timeConsume = make(map[int64]time.Time)
	e2eLatencyMicros = 0
	numRecords = 0
	var err error
	// clean up old files
	err = os.RemoveAll("log")
	if err != nil {
		log.Errorf("%v", err)
	}
	// read configuration file
	viper.SetConfigFile("../../.scalog.yaml")
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

	wg.Add(5)
	for i := 0; i < 4; i++ {
		c, err := client.NewClientWithShardingHint(dataAddr, discAddr, numReplica, int64(i))
		if err != nil {
			log.Fatalf("%v", err)
		}
		go appendThread(c, i)
	}

	for i := 0; i < 1; i++ {
		c, err := client.NewClient(dataAddr, discAddr, numReplica)
		if err != nil {
			log.Fatalf("%v", err)
		}
		go subscribeThread(c, i)
	}

	wg.Wait()

	// calculate e2e latency
	for gsn, appendTime := range timeAppend {
		consumeTime, ok := timeConsume[gsn]
		if !ok {
			continue
		}
		e2eLatencyMicros += float64(consumeTime.Sub(appendTime).Microseconds())
	}
	e2eLatencyMicros /= float64(len(timeAppend))
	fmt.Println("Average e2e latency: ", e2eLatencyMicros, " microseconds")
}
