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
var runTimeSecs int

var burstClient *scalog_api.Scalog = nil

func appendThread(scalog *scalog_api.Scalog, id int, shardId int, timeSecs int) {
	defer wg.Done()

	burstTimer := time.NewTimer(10 * time.Second)
	burstSize := int32(30)
	if shardId == 0 && id == 0 {
		burstClient = scalog_api.CreateBurstClient(0, "../../.scalog.yaml", burstSize)
	}

	numRecords := 0
	ticker := time.After(time.Duration(timeSecs) * time.Second)
	for {
		str := strings.Repeat("a", 4096)
		_ = scalog.AppendToAssignedShard(int32(shardId), str)
		if numRecords%1000 == 0 {
			log.Printf("[lagfix]: client %v appended %v records", id, numRecords)
		}
		numRecords++

		select {
		case <-ticker:
			log.Printf("[lagfix]: stopping client %v", id)
			scalog.StopAck <- true
			if shardId == 0 && id == 0 {
				log.Printf("printing burst record gsns")
				for gsn := range burstClient.Stats.AppendStartTime {
					log.Printf("gsn: %v", gsn)
				}
			}
			return
		case <-burstTimer.C:
			if shardId == 0 && id == 0 {
				log.Printf("generating burst of %v records", burstSize)
				record := strings.Repeat("b", 4096)
				for i := int32(0); i < burstSize; i++ {
					_ = burstClient.AppendToAssignedShard(0, record)
				}
			}
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
	// initialize global data structures
	runTimeSecs, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Errorf("[lagfix]: unable to parse time duration")
	}

	shardId, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Errorf("[lagfix]: unable to parse shard id")
	}

	numAppenders, err := strconv.Atoi(os.Args[3])
	if err != nil {
		log.Errorf("[lagfix]: unable to parse number of appenders")
	}

	filepath := os.Args[4]

	log.Printf("[lagfix]: starting benchmark with runtime %v", runTimeSecs)

	appendClients := make([]*scalog_api.Scalog, 0)
	// start producer
	wg.Add(numAppenders)
	for i := 0; i < numAppenders; i++ {
		c := scalog_api.CreateClient(1000, shardId, "../../.scalog.yaml") // assuming that 5 clients will be spawned per producer node
		go appendThread(c, i, shardId, runTimeSecs)
		appendClients = append(appendClients, c)
	}
	wg.Wait()

	// append latencies
	appendLatencies := make([]LatencyTuple, 0)
	appendStartTimeMap := make(map[int64]time.Time)
	appendEndTimeMap := make(map[int64]time.Time)

	appendMetrics, err := os.Create(filepath + "append_metrics_" + strconv.Itoa(shardId) + ".csv")
	if err != nil {
		log.Errorf("[lagfix]: failed to open csv file")
	}
	defer appendMetrics.Close()

	appendLatencyTimestamp, err := os.Create(filepath + "append_latency_timestamp_" + strconv.Itoa(shardId) + ".csv")
	if err != nil {
		log.Errorf("[lagfix]: failed to open csv file")
	}
	defer appendLatencyTimestamp.Close()

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

	if burstClient != nil {
		for gsn, appendStartTime := range burstClient.Stats.AppendStartTime {
			appendEndTime, ok := burstClient.Stats.AppendEndTime[gsn]
			if ok {
				appendLatencies = append(appendLatencies, LatencyTuple{gsn, appendEndTime.Sub(appendStartTime).Microseconds()})
				appendStartTimeMap[gsn] = appendStartTime
				appendEndTimeMap[gsn] = appendEndTime
			}
		}
		appendTput += float64(len(burstClient.Stats.AppendEndTime)) / float64(runTimeSecs)
	}

	// sort slices
	sort.Slice(appendLatencies, func(i, j int) bool {
		return appendLatencies[i].GSN < appendLatencies[j].GSN
	})

	// write to csv
	appendWriter := csv.NewWriter(appendMetrics)
	header := []string{"gsn", "latency (us)", "append tput (ops/sec)"}
	appendWriter.Write(header)
	for _, latency := range appendLatencies {
		appendWriter.Write([]string{strconv.Itoa(int(latency.GSN)), strconv.Itoa(int(latency.Latency)), strconv.FormatFloat(appendTput, 'f', -1, 64)})
	}
	appendWriter.Flush()

	appendTimestampWriter := csv.NewWriter(appendLatencyTimestamp)
	header = []string{"gsn", "timestamp", "latency (us)"}
	appendTimestampWriter.Write(header)
	for _, latency := range appendLatencies {
		formattedTime := appendEndTimeMap[latency.GSN].Format("15:04:05.000000")
		appendTimestampWriter.Write([]string{strconv.Itoa(int(latency.GSN)), formattedTime, strconv.Itoa(int(latency.Latency))})
	}
	appendTimestampWriter.Flush()

	log.Printf("[lagfix]: benchmark complete")
}
