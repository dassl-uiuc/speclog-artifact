package main

import (
	"encoding/csv"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/scalog_api"
)

// Path: benchmarking/scripts/reconfig.go
// Compare this snippet from benchmarking/scripts/single_client_e2e.go:
// here consumers read **all** the log records, each appender node operates at a max of 2.5k ops/sec (total 10k)

const NumberOfRequest = 10
const NumberOfBytes = 100
const StreamName = "AppendBenchmark"

const readType = "subscribe"

// global data structures to sync between readers and writers

var wg sync.WaitGroup

// time at which a record was computed on
var timeCompute map[int64]time.Time
var timeBeginCompute map[int64]time.Time
var computationTimeUs int64
var runTimeSecs int

// global stop signal
var stop chan bool

func appendThread(scalog *scalog_api.Scalog, id int, shardId int, timeSecs int, clientId int) {
	defer wg.Done()
	numRecords := 0
	ticker := time.After(time.Duration(timeSecs) * time.Second)
	for {
		var str string
		if clientId == 2 {
			str = strings.Repeat("b", 4096)
		} else {
			str = strings.Repeat("a", 4096)
		}
		_ = scalog.QuotaExpAppendToAssignedShard(int32(shardId), str, clientId, numRecords)
		if numRecords%1000 == 0 {
			log.Printf("[quota_change]: client %v appended %v records", id, numRecords)
		}
		numRecords++
		select {
		case <-ticker:
			log.Printf("[quota_change]: stopping client %v", id)
			scalog.StopAck <- true
			return
		default:
			continue
		}
	}
}

func computationThread(c *scalog_api.Scalog) {
	totalBatchSize := float64(0)
	numBatches := float64(0)
	printTicker := time.NewTicker(1 * time.Second)
	nextExpectedOffset := int64(0)
	for {
		latestOffset := c.GetLatestOffset()
		latestOffset -= 1
		if latestOffset >= nextExpectedOffset {
			lenBatch := latestOffset - nextExpectedOffset + 1
			start := time.Now()
			for i := nextExpectedOffset; i <= latestOffset; i++ {
				timeBeginCompute[c.Read(i).GSN] = start
			}
			for time.Now().Sub(start).Microseconds() < computationTimeUs {
				continue
			}
			for i := nextExpectedOffset; i <= latestOffset; i++ {
				timeCompute[c.Read(i).GSN] = time.Now()
			}
			totalBatchSize += float64(lenBatch)
			numBatches++

			nextExpectedOffset = latestOffset + 1
		}
		select {
		case <-printTicker.C:
			log.Printf("[quota_change]: computed %v records", totalBatchSize)
			log.Printf("[quota_change]: average batch size %v", totalBatchSize/numBatches)
		case <-stop:
			log.Printf("[quota_change]: consumer thread terminating")
			c.Stop <- true
			return
		default:
			continue
		}
	}
}

type LatencyTuple struct {
	GSN     int64
	Latency int64
}

func main() {
	timeCompute = make(map[int64]time.Time, 30000)
	timeBeginCompute = make(map[int64]time.Time, 30000)
	stop = make(chan bool)

	runTimeSecs, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Errorf("[quota_change]: unable to parse time duration")
	}

	numAppendersPerShard, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Errorf("[quota_change]: unable to parse number of appenders per shard")
	}

	newClientJoinTime, err := strconv.Atoi(os.Args[3])
	if err != nil {
		log.Errorf("[quota_change]: unable to parse new client join time")
	}

	computationTime, err := strconv.Atoi(os.Args[4])
	if err != nil {
		log.Errorf("[quota_change]: unable to parse time duration")
	}
	computationTimeUs = int64(computationTime)

	filepath := os.Args[5]

	log.Printf("[quota_change]: starting benchmark with runtime %v", runTimeSecs)

	var consumer *scalog_api.Scalog
	// start consumer
	for i := 0; i < 1; i++ {
		consumer = scalog_api.CreateClient(0, -1, "../../.scalog.yaml")
		consumer.Subscribe(0)
		go computationThread(consumer)
	}	

	appendClients := make([]*scalog_api.Scalog, 0)
	// start producer
	wg.Add(numAppendersPerShard * 2)
	for i := 0; i < numAppendersPerShard*2; i++ {
		shardId := i % 2
		c := scalog_api.CreateClient(1000, shardId, "../../.scalog.yaml")
		go appendThread(c, i, shardId, runTimeSecs, 0)
		appendClients = append(appendClients, c)
	}

	time.Sleep(time.Duration(newClientJoinTime) * time.Second)
	wg.Add(numAppendersPerShard)
	shardId := 0
	newClientRunTime := runTimeSecs - newClientJoinTime
	for i := 0; i < numAppendersPerShard; i++ {
		c := scalog_api.CreateClient(1000, shardId, "../../.scalog.yaml")
		go appendThread(c, i, shardId, newClientRunTime, 2)
		appendClients = append(appendClients, c)
	}

	wg.Wait()

	stop <- true

	// append latencies
	appendLatencies := make([]LatencyTuple, 0)
	appendStartTimeMap := make(map[int64]time.Time)
	appendEndTimeMap := make(map[int64]time.Time)

	appendMetrics, err := os.Create(filepath + "append_metrics.csv")
	if err != nil {
		log.Errorf("[quota_change]: failed to open csv file")
	}
	defer appendMetrics.Close()

	appendTput := float64(0)
	for _, appendClient := range appendClients {
		for gsn, appendStartTime := range appendClient.Stats.AppendStartTime {
			appendEndTime, ok := appendClient.Stats.AppendEndTime[gsn]
			if ok {
				appendLatencies = append(appendLatencies, LatencyTuple{gsn, appendEndTime.Sub(appendStartTime).Microseconds()})
				appendStartTimeMap[gsn] = appendStartTime
				appendEndTimeMap[gsn] = appendEndTime
			}
		}
		appendTput += float64(len(appendClient.Stats.AppendEndTime)) / float64(runTimeSecs)
	}

	// sort slices
	sort.Slice(appendLatencies, func(i, j int) bool {
		return appendLatencies[i].GSN < appendLatencies[j].GSN
	})

	// write to csv
	appendWriter := csv.NewWriter(appendMetrics)
	header := []string{"gsn", "latency (us)", "timestamp", "append tput (ops/sec)"}
	appendWriter.Write(header)
	for _, latency := range appendLatencies {
		formattedTime := appendEndTimeMap[latency.GSN].Format("15:04:05.000000")
		appendWriter.Write([]string{strconv.Itoa(int(latency.GSN)), strconv.Itoa(int(latency.Latency)), formattedTime, strconv.FormatFloat(appendTput, 'f', -1, 64)})
	}
	appendWriter.Flush()

	e2eMetrics, err := os.Create(filepath + "e2e_metrics.csv")
	if err != nil {
		log.Errorf("[quota_change]: failed to open csv file")
	}
	defer e2eMetrics.Close()

	e2eWriter := csv.NewWriter(e2eMetrics)
	header = []string{"gsn", "delivery latency (us)", "confirm latency (us)", "compute latency (us)", "e2e latency (us)", "queuing delay (us)", "delivery timestamp"}
	e2eWriter.Write(header)
	for _, latency := range appendLatencies {
		confirmTime, inConfirm := consumer.Stats.ConfirmTime[latency.GSN]
		computeTime, inCompute := timeCompute[latency.GSN]
		deliveryTime, inDelivery := consumer.Stats.DeliveryTime[latency.GSN]
		timeStamp := deliveryTime.Format("15:04:05.000000")
		if inConfirm && inCompute && inDelivery {
			e2eWriter.Write([]string{strconv.Itoa(int(latency.GSN)), strconv.Itoa(int(deliveryTime.Sub(appendStartTimeMap[latency.GSN]).Microseconds())), strconv.Itoa(int(confirmTime.Sub(appendStartTimeMap[latency.GSN]).Microseconds())), strconv.Itoa(int(computeTime.Sub(appendStartTimeMap[latency.GSN]).Microseconds())), strconv.Itoa(int(max(confirmTime.Sub(appendStartTimeMap[latency.GSN]).Microseconds(), computeTime.Sub(appendStartTimeMap[latency.GSN]).Microseconds()))), strconv.Itoa(int(timeBeginCompute[latency.GSN].Sub(consumer.Stats.DeliveryTime[latency.GSN]).Microseconds())), timeStamp})
		}
	}
	e2eWriter.Flush()

	log.Printf("[quota_change]: benchmark complete")
}
