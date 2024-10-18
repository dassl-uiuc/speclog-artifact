package main

import (
	"github.com/scalog/scalog/scalog_api"
	"fmt"
	"time"
	"os"
	"strconv"
	"encoding/json"
	"log"
	// "sync"
	"github.com/spf13/viper"
)

var appendThroughputFilePath = "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/analytics/append_throughput.txt"
var intrusionDetectionConfigFilePath = "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/intrusion_detection_config.yaml"

func Ping(appenderId int32) {
	// read configuration file
	viper.SetConfigFile(intrusionDetectionConfigFilePath)
	viper.AutomaticEnv()
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println("read config file error: %v", err)
	}
	runTime := int64(viper.GetInt("produce-run-time"))

	// defer wg.Done()

	scalogApi := scalog_api.CreateClient()

	recordsProduced := 0
	startTimeInSeconds := time.Now().Unix()
	startThroughputTimer := time.Now().UnixNano()
	for (time.Now().Unix() - startTimeInSeconds < runTime) {
		record := map[string]interface{}{
			"timestamp": time.Now().UnixNano(),
			"message":   "Intrusion detected from: " + string(appenderId),
		}
		recordJson, err := json.Marshal(record)
		if err != nil {
			fmt.Println("Error marshalling record")
			return
		}

		scalogApi.AppendToAssignedShard(appenderId, string(recordJson))

		recordsProduced++
	}

	// Calculate throughput
	endThroughputTimer := time.Now().UnixNano()
	throughput := float64(recordsProduced) / float64((endThroughputTimer - startThroughputTimer) / 1000000000)
	file, err := os.OpenFile(appendThroughputFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// write the throughput value
	if _, err := file.WriteString(fmt.Sprintf("%f\n", throughput)); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Produced ", recordsProduced, " records")
}

func main() {
	fmt.Println("Running intrusion detection generator")

	if len(os.Args) < 2 {
		fmt.Println("Please provide appender id")
		return
	}

	appenderId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Invalid reader id. It should be a number.")
		return
	}

	// var wg sync.WaitGroup
	// numThreads := 10
	// for i := 0; i < numThreads; i++ {
	// 	wg.Add(1)
	// 	go Ping(int32(appenderId), &wg)
	// }
	// wg.Wait()

	Ping(int32(appenderId))
}