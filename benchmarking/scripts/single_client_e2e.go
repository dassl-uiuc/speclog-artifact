package main

import (
	"fmt"
	"os"
	"strconv"
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

/*
Flow of a record:
1. Record is appended to the shared log
2. Record is consumed from the shared log through a subscribe and asynchronously posted on a computation channel
3. Record is read by the computation thread. This thread can batch records together. Currently, as long as the batch size < 5, the computation thread sleeps for a fixed amount of time (configurable) before marking the record as computed.
4. A background confirmation thread reads confirmation records from the shared log. This is completely in parallel with the actual computation
*/

// global data structures to sync between readers and writers
var wg sync.WaitGroup
var timeAppend map[int64]time.Time
var timeAppendMu sync.Mutex

// time at which a record was consumed from the shared log
var timeConsume map[int64]time.Time

// time at which a record was computed on
var timeCompute map[int64]time.Time
var computationTimeUs int64

var runTimeSecs int

// computation channel
var computationChan chan *client.CommittedRecord

// global stop signal
var stop chan bool

func appendThread(client *client.Client, id int, timeSecs int) {
	defer wg.Done()
	numRecords := 0
	ticker := time.After(time.Duration(timeSecs) * time.Second)
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
		if numRecords%1000 == 0 {
			log.Printf("[single_client_e2e]: client %v appended %v records", id, numRecords)
		}
		if err != nil {
			log.Errorf("%v", err)
		}
		numRecords++
		select {
		case <-ticker:
			log.Printf("[single_client_e2e]: client %v appended %v records at avg latency of %v microseconds", id, numRecords, appendLatencyAvgMicros/float64(numRecords))
			return
		default:
			continue
		}
	}
}

func computationThread() {
	totalBatchSize := float64(0)
	numBatches := float64(0)
	printTicker := time.NewTicker(1 * time.Second)
	batch := make([]*client.CommittedRecord, 0)
	totalBatchingLatency := float64(0)
	for {
		startBatch := time.Now()
	batcher:
		for {
			select {
			case r := <-computationChan:
				batch = append(batch, r)
			case <-stop:
				return
			case <-printTicker.C:
				log.Printf("[single_client_e2e]: average batch size: %v", totalBatchSize/numBatches)
				log.Printf("[single_client_e2e]: average batching latency: %v", totalBatchingLatency/numBatches)
			default:
				break batcher
			}
		}
		totalBatchingLatency += float64(time.Now().Sub(startBatch).Microseconds())

		if len(batch) > 0 {
			if len(batch) < 15 {
				start := time.Now()
				for time.Now().Sub(start).Microseconds() < computationTimeUs {
					continue
				}
				for _, r := range batch {
					timeCompute[r.GSN] = time.Now()
				}
				totalBatchSize += float64(len(batch))
				numBatches++
				batch = make([]*client.CommittedRecord, 0)
			} else {
				log.Printf("[single_client_e2e]: warning! batch size %v is greater than 15", len(batch))
				batch = make([]*client.CommittedRecord, 0)
			}
		}
	}
}

func subscribeThread(cli *client.Client, id int, timeSec int) {
	defer wg.Done()
	stream, err := cli.Subscribe(0)
	stop = make(chan bool)
	defer close(stop)
	ticker := time.After(time.Duration(timeSec) * time.Second)
	if err != nil {
		log.Errorf("%v", err)
	}
	consumed := 0
	prevGsn := int64(-1)

	for {
		select {
		case r := <-stream:
			if r.GSN != prevGsn+1 {
				log.Errorf("[single_client_e2e]: out of order record: %v", r.GSN)
			}
			prevGsn = r.GSN
			timeConsume[r.GSN] = time.Now()
			if r.Record != "0xDEADBEEF" {
				computationChan <- &r
			}
			consumed++
			continue
		case <-ticker:
			log.Printf("[single_client_e2e]: consumed %v records", consumed)
			return
		}
	}
}

func main() {
	// initialize global data structures
	timeAppend = make(map[int64]time.Time, 30000)
	timeConsume = make(map[int64]time.Time, 30000)
	timeCompute = make(map[int64]time.Time, 30000)
	computationChan = make(chan *client.CommittedRecord, 4096)
	computationTime, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Errorf("[single_client_e2e]: unable to parse time duration")
	}
	computationTimeUs = int64(computationTime)

	runTimeSecs, err = strconv.Atoi(os.Args[2])
	if err != nil {
		log.Errorf("[single_client_e2e]: unable to parse time duration")
	}

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
		log.Errorf("[single_client_e2e]: read config file error: %v", err)
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

	log.Printf("[single_client_e2e]: starting benchmark with runtime %v", runTimeSecs)
	wg.Add(11)
	for i := 0; i < 10; i++ {
		c, err := client.NewClientWithShardingHint(dataAddr, discAddr, numReplica, int64(i))
		if err != nil {
			log.Fatalf("%v", err)
		}
		go appendThread(c, i, runTimeSecs)
	}

	for i := 0; i < 1; i++ {
		c, err := client.NewClient(dataAddr, discAddr, numReplica)
		if err != nil {
			log.Fatalf("%v", err)
		}
		go subscribeThread(c, i, runTimeSecs)
		go computationThread()

	}

	wg.Wait()

	// dump statistics to calculate e2e latencies
	computableRecords := 0
	totalRecords := 0
	for gsn, appendTime := range timeAppend {
		if totalRecords > 200 {
			consumeTime, ok := timeConsume[gsn]
			if !ok {
				continue
			}
			computeTime, ok := timeCompute[gsn]
			if !ok {
				continue
			}

			log.Printf("latencies: %v, %v", consumeTime.Sub(appendTime).Microseconds(), computeTime.Sub(appendTime).Microseconds())
			computableRecords++
		}
		totalRecords++
	}
}
