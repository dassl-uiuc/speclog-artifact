package main

import (
	"github.com/scalog/scalog/scalog_api"
	"fmt"
	"time"
	"os"
	"strconv"
	"encoding/json"
	"log"
	"github.com/spf13/viper"
)

var e2eLatenciesFilePath = "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/analytics/e2e_latencies.txt"
var readThroughputFilePath = "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/analytics/read_throughput.txt"
var intrusionDetectionConfigFilePath = "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/intrusion_detection_config.yaml"
var processingTime = int64(2000)

// TODO: Add computation here
func HandleIntrusion() {
	// fmt.Println("Handling intrusion")
	processingStartTime := time.Now().UnixMicro()
	for time.Now().UnixMicro() - processingStartTime < processingTime {
		// Do nothing
	}
}

func IntrusionDetectionProcessing(readerId int32) {
	// read configuration file
	viper.SetConfigFile(intrusionDetectionConfigFilePath)
	viper.AutomaticEnv()
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println("read config file error: %v", err)
	}
	runTime := int64(viper.GetInt("consume-run-time"))

	scalogApi := scalog_api.CreateClient()

	scalogApi.SubscribeToAssignedShard(readerId)

	var record map[string]interface{}
	// Time used to keep track of the time to run wordcount
	startTimeInSeconds := time.Now().Unix()
	prevOffset := int64(0)
	recordsReceived := 0
	e2eLatencies := make([]int64, 1000000)
	startThroughputTimer := time.Now().UnixNano()
	for (time.Now().Unix() - startTimeInSeconds < (runTime)) {
		offset := scalogApi.GetLatestOffset()
		if offset != prevOffset {
			for i := prevOffset; i < offset; i++ {
				recordJson := scalogApi.Read(i)

				err := json.Unmarshal([]byte(recordJson), &record)
				if err != nil {
					fmt.Println("Error unmarshalling record")
					return
				}

				// HandleIntrusion()

				// Calculate end-to-end latency
				e2eLatencies[recordsReceived] = time.Now().UnixNano() - int64(record["timestamp"].(float64))

				recordsReceived++
			}

			prevOffset = offset
		}
	}

	endThroughputTimer := time.Now().UnixNano()
	fmt.Println("Received ", recordsReceived, " records")

	// Calculate latency
	totalE2ELatency := int64(0)
	for i := 0; i < recordsReceived; i++ {
		totalE2ELatency += e2eLatencies[i]
	}
	avgE2ELatency := float64(totalE2ELatency) / float64(recordsReceived) / 1000

	file, err := os.OpenFile(e2eLatenciesFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	if _, err := file.WriteString(fmt.Sprintf("%f\n", avgE2ELatency)); err != nil {
		log.Fatal(err)
	}

	// Calculate throughput
	throughput := float64(recordsReceived) / float64((endThroughputTimer - startThroughputTimer) / 1000000000)
	file, err = os.OpenFile(readThroughputFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// write the throughput value
	if _, err := file.WriteString(fmt.Sprintf("%f\n", throughput)); err != nil {
		log.Fatal(err)
	}

	// Purpose is to wait for producers to finish appending and write their stats to files
	endingTimeout := 10
	startTimeInSeconds = time.Now().Unix()
	for (time.Now().Unix() - startTimeInSeconds < (int64(endingTimeout))) {
		// wait for the remaining time to elapse
	}
}

func main() {
	fmt.Println("Running intrusion detection application")

	if len(os.Args) < 2 {
		fmt.Println("Please provide reader id")
		return
	}

	readerId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Invalid reader id. It should be a number.")
		return
	}

	IntrusionDetectionProcessing(int32(readerId))
}