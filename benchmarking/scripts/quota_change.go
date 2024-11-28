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
		_ = scalog.AppendToAssignedShard(int32(shardId), str)
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

type LatencyTuple struct {
	GSN     int64
	Latency int64
}

func main() {
	runTimeSecs, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Errorf("[quota_change]: unable to parse time duration")
	}

	shardId, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Errorf("[quota_change]: unable to parse shard id")
	}

	numAppenders, err := strconv.Atoi(os.Args[3])
	if err != nil {
		log.Errorf("[quota_change]: unable to parse number of appenders")
	}

	clientId, err := strconv.Atoi(os.Args[4])
	if err != nil {
		log.Errorf("[quota_change]: unable to parse client id")
	}

	filepath := os.Args[5]

	log.Printf("[quota_change]: starting benchmark with runtime %v", runTimeSecs)

	appendClients := make([]*scalog_api.Scalog, 0)
	// start producer
	wg.Add(numAppenders)
	for i := 0; i < numAppenders; i++ {
		c := scalog_api.CreateClient(1000, shardId, "../../.scalog.yaml")
		go appendThread(c, i, shardId, runTimeSecs, clientId)
		appendClients = append(appendClients, c)
	}
	wg.Wait()

	// append latencies
	appendLatencies := make([]LatencyTuple, 0)
	appendStartTimeMap := make(map[int64]time.Time)
	appendEndTimeMap := make(map[int64]time.Time)

	appendMetrics, err := os.Create(filepath + "append_metrics_" + strconv.Itoa(clientId) + "_" + strconv.Itoa(shardId) + ".csv")
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

	log.Printf("[quota_change]: benchmark complete")
}
