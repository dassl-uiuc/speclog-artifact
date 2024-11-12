package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
	"context"

	"github.com/scalog/scalog/scalog_api"
	"github.com/scalog/scalog/benchmark/util"
	// "sync"
	"github.com/spf13/viper"
	rateLimiter "golang.org/x/time/rate"
)

var intrusionDetectionConfigFilePath = "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/intrusion_detection_config.yaml"

func Append_One_Ping(appenderId int32) {
	// read configuration file
	viper.SetConfigFile(intrusionDetectionConfigFilePath)
	viper.AutomaticEnv()
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println("read config file error: %v", err)
	}
	runTime := int64(viper.GetInt("produce-run-time"))

	scalogApi := scalog_api.CreateClient()

	recordsProduced := 0
	startTimeInSeconds := time.Now().Unix()
	startThroughputTimer := time.Now().UnixNano()
	for time.Now().Unix()-startTimeInSeconds < runTime {
		record := map[string]interface{}{
			"timestamp": time.Now().UnixNano(),
			"message":   util.GenerateRandomString(1024),
			"click":     rand.Intn(10),
		}
		recordJson, err := json.Marshal(record)
		if err != nil {
			fmt.Println("Error marshalling record")
			return
		}

		scalogApi.AppendOneToAssignedShard(appenderId, string(recordJson))

		recordsProduced++
	}

	// Calculate throughput
	endThroughputTimer := time.Now().UnixNano()
	throughput := float64(recordsProduced) / float64((endThroughputTimer-startThroughputTimer)/1000000000)
	appendThroughputFilePath := "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/data/append_throughput_" + strconv.Itoa(int(appenderId)) + ".txt"
	file, err := os.OpenFile(appendThroughputFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// write the throughput value
	if _, err := file.WriteString(fmt.Sprintf("%f\n", throughput)); err != nil {
		log.Fatal(err)
	}

	// Record records produced
	appendRecordsProducedFilePath := "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/data/append_records_produced_" + strconv.Itoa(int(appenderId)) + ".txt"
	file, err = os.OpenFile(appendRecordsProducedFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// write the records produced
	if _, err := file.WriteString(fmt.Sprintf("%d\n", recordsProduced)); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Produced ", recordsProduced, " records")
}

func Append_Stream_Ping(appenderId int32, clientNumber int) {
	// read configuration file
	viper.SetConfigFile(intrusionDetectionConfigFilePath)
	viper.AutomaticEnv()
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println("read config file error: %v", err)
	}
	runTime := int64(viper.GetInt("produce-run-time"))

	scalogApi := scalog_api.CreateClient()
	stop := make(chan bool)
	go scalogApi.Ack(stop)

	recordsProduced := 0
	limiter := rateLimiter.NewLimiter(rateLimiter.Limit(300), 1)
	startTimeInSeconds := time.Now().Unix()
	startThroughputTimer := time.Now().UnixNano()
	for time.Now().Unix()-startTimeInSeconds < runTime {
		err := limiter.Wait(context.Background())
		if err != nil {
			fmt.Errorf("rate limiter error: ", err)
			return
		}

		record := map[string]interface{}{
			"timestamp": time.Now().UnixNano(),
			"message":   util.GenerateRandomString(1024),
			"click":     rand.Intn(10),
		}
		recordJson, err := json.Marshal(record)
		if err != nil {
			fmt.Println("Error marshalling record")
			return
		}

		scalogApi.AppendToAssignedShard(appenderId, string(recordJson))

		recordsProduced++
	}

	close(stop)

	// Calculate throughput
	endThroughputTimer := time.Now().UnixNano()
	throughput := float64(recordsProduced) / float64((endThroughputTimer-startThroughputTimer)/1000000000)
	appendThroughputFilePath := "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/data/append_throughput_" + strconv.Itoa(int(appenderId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
	file, err := os.OpenFile(appendThroughputFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// write the throughput value
	if _, err := file.WriteString(fmt.Sprintf("%f\n", throughput)); err != nil {
		log.Fatal(err)
	}

	// Record records produced
	appendRecordsProducedFilePath := "/proj/rasl-PG0/tshong/speclog/applications/vanilla_applications/intrusion_detection/data/append_records_produced_" + strconv.Itoa(int(appenderId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
	file, err = os.OpenFile(appendRecordsProducedFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// write the records produced
	if _, err := file.WriteString(fmt.Sprintf("%d\n", recordsProduced)); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Produced ", recordsProduced, " records")
}

func main() {
	fmt.Println("Running intrusion detection generator")

	if len(os.Args) < 4 {
		fmt.Println("Usage: go run intrusion_detection_generator.go <appender_id> <append_type> <client_number>")
		return
	}

	appenderId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Invalid reader id. It should be a number.")
		return
	}

	appendType, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Println("Invalid append type. It should be a number.")
		return
	}

	clientNumber, err := strconv.Atoi(os.Args[3])
	if err != nil {
		fmt.Println("Invalid client number. It should be a number.")
		return
	}

	if appendType == 0 {
		Append_One_Ping(int32(appenderId))
	} else {
		Append_Stream_Ping(int32(appenderId), clientNumber)
	}
}
