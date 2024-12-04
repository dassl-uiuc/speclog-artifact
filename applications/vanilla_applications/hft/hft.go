package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/scalog/scalog/client"
	"github.com/scalog/scalog/scalog_api"
	"github.com/spf13/viper"

	_ "github.com/mattn/go-sqlite3"
)

var transactionAnalysisConfigFilePath = "../../applications/vanilla_applications/hft/hft_config.yaml"

// TODO: Add computation here
func Compute() {
}

func HftProcessing(readerId int32, readerId2 int32, clientNumber int) {
	// read configuration file
	fmt.Printf("readerId: %d, readerId2: %d\n", readerId, readerId2)
	viper.SetConfigFile(transactionAnalysisConfigFilePath)
	viper.AutomaticEnv()
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println("read config file error: %v", err)
	}
	runTime := int64(viper.GetInt("consume-run-time"))
	numReadClients := int32(viper.GetInt("num-read-clients"))

	filterValue := numReadClients
	scalogApi := scalog_api.CreateClient(1000, -1, "/proj/rasl-PG0/JiyuHu23/speclog/.scalog.yaml")

	scalogApi.FilterSubscribeDouble(0, readerId, readerId2, filterValue)

	// var record map[string]interface{}
	// Time used to keep track of the time to run wordcount
	startTimeInSeconds := time.Now().Unix()
	prevOffset := int64(0)
	recordsReceived := 0
	computeE2eEndTimes := make(map[int64]int64)
	timeBeginCompute := make(map[int64]time.Time)
	batchesReceived := 0
	batchSize := 0
	transactionAnalysisLatencies := 0

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
			committedRecords1 := make([]client.CommittedRecord, 0)
			committedRecords2 := make([]client.CommittedRecord, 0)

			startComputeTime := time.Now()
			for i := prevOffset; i < offset; i++ {
				committedRecord := scalogApi.Read(i)

				if committedRecord.RecordId % clientNumber == readerId:
					committedRecords1 = append(committedRecords1, committedRecord)
				else:
					committedRecords2 = append(committedRecords2, committedRecord)

				timeBeginCompute[committedRecord.GSN] = startComputeTime
			}

			// fmt.Println("Length of committed records: ", len(committedRecords))
			// fmt.Println("Expected number of records: ", offset-prevOffset)

			// startHandleIntrusion := time.Now()
			// HandleIntrusion(committedRecords, db)
			// handleIntrusionLatencies += int(time.Since(startHandleIntrusion).Nanoseconds())

			duration := 500 * time.Microsecond
			start := time.Now()
			for time.Since(start) < duration {
				// Busy-waiting
			}

			// Iterate through committed records
			timestamp := time.Now().UnixNano()
			for _, record := range committedRecords {
				computeE2eEndTimes[record.GSN] = timestamp

				recordsReceived++
			}

			batchesReceived++
			batchSize += len(committedRecords)

			prevOffset = offset
		}
	}

	endThroughputTimer := time.Now().UnixNano()

	// Wait for everyone to finish their run
	time.Sleep(30 * time.Second)
	scalogApi.Stop <- true

	// Record records received
	recordsReceivedFilePath := "../../applications/vanilla_applications/hft/data/records_received_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
	file, err := os.OpenFile(recordsReceivedFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	if _, err := file.WriteString(fmt.Sprintf("%d\n", recordsReceived)); err != nil {
		log.Fatal(err)
	}

	// Record transaction analysis latencies
	transactionAnalysisLatenciesFilePath := "../../applications/vanilla_applications/hft/data/hft_latencies_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
	file, err = os.OpenFile(transactionAnalysisLatenciesFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	averageTransactionAnalysisLatencies := float64(transactionAnalysisLatencies) / float64(batchesReceived)
	if _, err := file.WriteString(fmt.Sprintf("%f\n", averageTransactionAnalysisLatencies)); err != nil {
		log.Fatal(err)
	}

	// Dump compute e2e latencies
	computeE2eEndTimesFilePath := "../../applications/vanilla_applications/hft/data/compute_e2e_end_times_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
	file, err = os.OpenFile(computeE2eEndTimesFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	for gsn, time := range computeE2eEndTimes {
		if _, err := file.WriteString(fmt.Sprintf("%d,%d\n", gsn, time)); err != nil {
			log.Fatal(err)
		}
	}

	// Dump delivery latencies with GSNs
	deliveryLatenciesFilePath := "../../applications/vanilla_applications/hft/data/delivery_latencies_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
	file, err = os.OpenFile(deliveryLatenciesFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	deliveryTimes := scalogApi.Stats.DeliveryTime
	for gsn, time := range deliveryTimes {
		if _, err := file.WriteString(fmt.Sprintf("%d,%d\n", gsn, time.UnixNano())); err != nil {
			log.Fatal(err)
		}
	}

	// Dump start compute times
	startComputeTimesFilePath := "../../applications/vanilla_applications/hft/data/start_compute_times_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
	file, err = os.OpenFile(startComputeTimesFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	for gsn, time := range timeBeginCompute {
		if _, err := file.WriteString(fmt.Sprintf("%d,%d\n", gsn, time.UnixNano())); err != nil {
			log.Fatal(err)
		}
	}

	// Dump batch sizes
	avgBatchSize := float64(batchSize) / float64(batchesReceived)
	batchSizesFilePath := "../../applications/vanilla_applications/hft/data/batch_sizes_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
	file, err = os.OpenFile(batchSizesFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	if _, err := file.WriteString(fmt.Sprintf("%f\n", avgBatchSize)); err != nil {
		log.Fatal(err)
	}

	// Dump confirm latencies with GSNs
	confirmLatenciesFilePath := "../../applications/vanilla_applications/hft/data/confirm_latencies_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
	file, err = os.OpenFile(confirmLatenciesFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	confirmTimes := scalogApi.Stats.ConfirmTime
	for gsn, time := range confirmTimes {
		if _, err := file.WriteString(fmt.Sprintf("%d,%d\n", gsn, time.UnixNano())); err != nil {
			log.Fatal(err)
		}
	}

	// Calculate throughput
	throughput := float64(recordsReceived) / float64((endThroughputTimer-startThroughputTimer)/1000000000)
	readThroughputFilePath := "../../applications/vanilla_applications/hft/data/read_throughput_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
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
	fmt.Println("Running hft application")

	if len(os.Args) < 3 {
		fmt.Println("Usage: go run hft.go <reader_id> <client_number>")
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

	HftProcessing(int32(readerId), int32(readerId)+1, clientNumber)
}
