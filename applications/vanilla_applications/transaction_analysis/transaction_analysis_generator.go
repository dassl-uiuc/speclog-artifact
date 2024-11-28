package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/scalog/scalog/benchmark/util"
	"github.com/scalog/scalog/scalog_api"

	// "sync"
	"github.com/spf13/viper"
)

var transactionAnalysisConfigFilePath = "../../applications/vanilla_applications/transaction_analysis/transaction_analysis_config.yaml"

func Append_One_Ping(appenderId int32, clientNumber int, offsetForShardingPolicy int) {
	// read configuration file
	viper.SetConfigFile(transactionAnalysisConfigFilePath)
	viper.AutomaticEnv()
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println("read config file error: %v", err)
	}
	runTime := int64(viper.GetInt("produce-run-time"))
	// numReadClients := int32(viper.GetInt("num-read-clients"))

	scalogApi := scalog_api.CreateClient(1000, offsetForShardingPolicy, "/proj/rasl-PG0/tshong/speclog/.scalog.yaml")

	recordsProduced := 0
	startTimeInSeconds := time.Now().Unix()
	startThroughputTimer := time.Now().UnixNano()
	for time.Now().Unix()-startTimeInSeconds < runTime {
		record := util.GenerateRandomString(4096)
		if err != nil {
			fmt.Println("Error marshalling record")
			return
		}

		// recordId := int32(rand.Int31n(numReadClients))
		scalogApi.FilterAppendOne(record, appenderId)

		recordsProduced++
	}

	// Calculate throughput
	endThroughputTimer := time.Now().UnixNano()

	WriteStats(recordsProduced, startThroughputTimer, endThroughputTimer, appenderId, clientNumber, scalogApi)
}

func Append_Stream_Ping(appenderId int32, clientNumber int, offsetForShardingPolicy int) {
	// read configuration file
	viper.SetConfigFile(transactionAnalysisConfigFilePath)
	viper.AutomaticEnv()
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println("read config file error: %v", err)
	}
	runTime := int64(viper.GetInt("produce-run-time"))
	// numReadClients := int32(viper.GetInt("num-read-clients"))

	rand.Seed(time.Now().UnixNano())
	scalogApi := scalog_api.CreateClient(1000, offsetForShardingPolicy, "/proj/rasl-PG0/tshong/speclog/.scalog.yaml")

	recordsProduced := 0
	startTimeInSeconds := time.Now().Unix()
	startThroughputTimer := time.Now().UnixNano()
	for time.Now().Unix()-startTimeInSeconds < runTime {
		record := util.GenerateRandomString(4096)
		if err != nil {
			fmt.Println("Error marshalling record")
			return
		}

		// recordId := int32(rand.Int31n(numReadClients))
		// scalogApi.FilterAppend(record, recordId)
		scalogApi.FilterAppend(record, appenderId)

		recordsProduced++
	}

	// Calculate throughput
	endThroughputTimer := time.Now().UnixNano()

	scalogApi.StopAck <- true
	WriteStats(recordsProduced, startThroughputTimer, endThroughputTimer, appenderId, clientNumber, scalogApi)
}

func WriteStats(recordsProduced int, startThroughputTimer int64, endThroughputTimer int64, appenderId int32, clientNumber int, scalogApi *scalog_api.Scalog) {
	// Wait for everyone to finish their run
	time.Sleep(30 * time.Second)

	throughput := float64(recordsProduced) / float64((endThroughputTimer-startThroughputTimer)/1000000000)
	appendThroughputFilePath := "../../applications/vanilla_applications/transaction_analysis/data/append_throughput_" + strconv.Itoa(int(appenderId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
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
	appendRecordsProducedFilePath := "../../applications/vanilla_applications/transaction_analysis/data/append_records_produced_" + strconv.Itoa(int(appenderId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
	file, err = os.OpenFile(appendRecordsProducedFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// write the records produced
	if _, err := file.WriteString(fmt.Sprintf("%d\n", recordsProduced)); err != nil {
		log.Fatal(err)
	}

	appendStartTimestampsFilePath := "../../applications/vanilla_applications/transaction_analysis/data/append_start_timestamps_" + strconv.Itoa(int(appenderId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
	file, err = os.OpenFile(appendStartTimestampsFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	appendStartTimes := scalogApi.Stats.AppendStartTime
	for gsn, time := range appendStartTimes {
		if _, err := file.WriteString(fmt.Sprintf("%d,%d\n", gsn, time.UnixNano())); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("Produced ", recordsProduced, " records")
}

func main() {
	fmt.Println("Running transaction analysis generator")

	if len(os.Args) < 5 {
		fmt.Println("Usage: go run transaction_analysis_generator.go <appender_id> <append_type> <client_number> <offset_for_sharding_policy>")
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

	offsetForShardingPolicy, err := strconv.Atoi(os.Args[4])
	if err != nil {
		fmt.Println("Invalid offset for sharding policy. It should be a number, err: ", err)
		return
	}

	fmt.Println("Offset for sharding policy for appender ", appenderId, " is ", offsetForShardingPolicy)

	if appendType == 0 {
		Append_One_Ping(int32(appenderId), clientNumber, offsetForShardingPolicy)
	} else {
		Append_Stream_Ping(int32(appenderId), clientNumber, offsetForShardingPolicy)
	}
}