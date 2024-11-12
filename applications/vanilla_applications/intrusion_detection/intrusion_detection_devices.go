package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/scalog/scalog/scalog_api"
	"github.com/spf13/viper"
)

var intrusionDetectionConfigFilePath = "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/intrusion_detection_config.yaml"
var processingTime = int64(2000)
var detectionTimeRangeNs = int64(10000)
var window = make([]map[string]interface{}, 1)
var clickRateThreshold = 5

// TODO: Add computation here
// func HandleIntrusion(record map[string]interface{}) {
// 	// fmt.Println("Handling intrusion")
// 	processingStartTime := time.Now().UnixMicro()

// 	var timestamp int64 = int64(record["timestamp"].(float64))

// 	window = append(window, record)
// 	var durationNs int64
// 	for {
// 		durationNs = timestamp - window[0]["timestamp"].(int64)
// 		if durationNs > detectionTimeRangeNs {
// 			window = window[1:] // remove the first element
// 		} else {
// 			break
// 		}
// 	}

// 	var totalClick int = 0
// 	for i := 0; i < len(window); i++ {
// 		totalClick += window[i]["click"].(int)
// 	}

// 	clickRate := totalClick / int(durationNs/1000000000)
// 	if clickRate > clickRateThreshold {
// 		fmt.Println("Intrusion detected")
// 	}
// }

func IntrusionDetectionProcessing(readerId int32, clientNumber int) {
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
	e2eLatencies := make([]int64, 10000000)

	// Wait for first record to come in before starting throughput timer
	for {
		offset := scalogApi.GetLatestOffset()
		if offset != prevOffset {
			break
		}
	}

	startThroughputTimer := time.Now().UnixNano()
	for time.Now().Unix()-startTimeInSeconds < (runTime) {
		offset := scalogApi.GetLatestOffset()
		if offset != prevOffset {
			for i := prevOffset; i < offset; i++ {
				recordJson := scalogApi.Read(i)

				err := json.Unmarshal([]byte(recordJson), &record)
				if err != nil {
					fmt.Println("Error unmarshalling record")
					return
				}

				// HandleIntrusion(record)

				// Calculate end-to-end latency
				e2eLatencies[recordsReceived] = time.Now().UnixNano() - int64(record["timestamp"].(float64))

				recordsReceived++
			}

			prevOffset = offset
		}
	}

	endThroughputTimer := time.Now().UnixNano()

	// Record records received
	recordsReceivedFilePath := "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/data/records_received_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
	file, err := os.OpenFile(recordsReceivedFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	if _, err := file.WriteString(fmt.Sprintf("%d\n", recordsReceived)); err != nil {
		log.Fatal(err)
	}

	// Calculate latency
	totalE2ELatency := int64(0)
	for i := 0; i < recordsReceived; i++ {
		totalE2ELatency += e2eLatencies[i]
	}
	avgE2ELatency := float64(totalE2ELatency) / float64(recordsReceived) / 1000

	e2eLatenciesFilePath := "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/data/e2e_latencies_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
	file, err = os.OpenFile(e2eLatenciesFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	if _, err := file.WriteString(fmt.Sprintf("%f\n", avgE2ELatency)); err != nil {
		log.Fatal(err)
	}

	// Calculate throughput
	throughput := float64(recordsReceived) / float64((endThroughputTimer-startThroughputTimer)/1000000000)
	readThroughputFilePath := "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/data/read_throughput_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
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
	for time.Now().Unix()-startTimeInSeconds < (int64(endingTimeout)) {
		// wait for the remaining time to elapse
	}

	fmt.Println("Received ", recordsReceived, " records")
}

func main() {
	fmt.Println("Running intrusion detection application")

	if len(os.Args) < 3 {
		fmt.Println("Usage: go run intrusion_detection_devices.go <reader_id> <client_number>")
		return
	}

	readerId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Invalid reader id. It should be a number.")
		return
	}

	clientNumber, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Println("Invalid client number. It should be a number.")
		return
	}

	IntrusionDetectionProcessing(int32(readerId), clientNumber)
}
