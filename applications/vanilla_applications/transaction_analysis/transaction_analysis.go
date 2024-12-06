package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/boltdb/bolt"
	"github.com/scalog/scalog/benchmark/util"
	"github.com/scalog/scalog/client"
	"github.com/scalog/scalog/scalog_api"
	"github.com/spf13/viper"

	_ "github.com/mattn/go-sqlite3"
)

var transactionAnalysisConfigFilePath = "../../applications/vanilla_applications/transaction_analysis/transaction_analysis_config.yaml"
var dbEntrySize = 2048
var padding = util.GenerateRandomString(dbEntrySize)

var gsnThreshold = int64(250000)

// TODO: Add computation here
func AnalyzeTransaction(committedRecords []client.CommittedRecord, db *bolt.DB) {
	err := db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("users"))
		if bucket == nil {
			log.Printf("Bucket 'users' does not exist")
			return nil
		}

		// Loop through the committed records and update each one
		for _, record := range committedRecords {
			userID := record.RecordID
			transactionAmount, err := strconv.Atoi(record.Record[0:4])
			if err != nil {
				log.Printf("Invalid transaction amount in record: %v", err)
				continue
			}

			userIDStr := fmt.Sprintf("%d", userID)
			key := []byte(userIDStr)
			value := bucket.Get(key)

			// If the user doesn't exist, insert the initial record
			if value == nil {
				userData := fmt.Sprintf("%d,%d,%d,%d,", transactionAmount, transactionAmount, transactionAmount, 1)
				userData = userData + padding[:dbEntrySize-len(userData)]

				err := bucket.Put(key, []byte(userData))
				if err != nil {
					log.Printf("Failed to insert new record for user %s: %v", userIDStr, err)
				}
				continue
			}

			existingValue := string(value)
			var avgTransaction, maxTransaction, minTransaction, numTransactions int
			_, err = fmt.Sscanf(existingValue, "%d,%d,%d,%d,", &avgTransaction, &maxTransaction, &minTransaction, &numTransactions)
			if err != nil {
				log.Printf("Failed to parse user data for user %s: %v", userIDStr, err)
				continue
			}

			totalTransactionAmount := float64(avgTransaction)*float64(numTransactions) + float64(transactionAmount)
			numTransactions++
			avgTransaction = int(totalTransactionAmount / float64(numTransactions))

			if transactionAmount > maxTransaction {
				maxTransaction = transactionAmount
			}
			if transactionAmount < minTransaction {
				minTransaction = transactionAmount
			}

			userData := fmt.Sprintf("%d,%d,%d,%d,", avgTransaction, maxTransaction, minTransaction, numTransactions)
			userData = userData + padding[:dbEntrySize-len(userData)]

			err = bucket.Put(key, []byte(userData))
			if err != nil {
				log.Printf("Failed to update record for user %s: %v", userIDStr, err)
			}
		}

		return nil
	})

	if err != nil {
		log.Printf("Failed to update database: %v", err)
	}
}

func CreateDatabase() *bolt.DB {
	dbFile := "/data/records.db"
	db, err := bolt.Open(dbFile, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("users"))
		return err
	})
	if err != nil {
		log.Fatal(err)
	}

	return db
}

func TransactionAnalysisProcessing(readerId int32, clientNumber int) {
	// Create database
	db := CreateDatabase()

	// read configuration file
	viper.SetConfigFile(transactionAnalysisConfigFilePath)
	viper.AutomaticEnv()
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println("read config file error: %v", err)
	}
	runTime := int64(viper.GetInt("consume-run-time"))
	numReadClients := int32(viper.GetInt("num-read-clients"))

	filterValue := numReadClients
	scalogApi := scalog_api.CreateClient(1000, -1, "/proj/rasl-PG0/tshong/speclog/.scalog.yaml")

	scalogApi.FilterSubscribe(0, readerId, filterValue)

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
			committedRecords := make([]client.CommittedRecord, 0)

			startComputeTime := time.Now()
			for i := prevOffset; i < offset; i++ {
				committedRecord := scalogApi.Read(i)

				committedRecords = append(committedRecords, committedRecord)

				timeBeginCompute[committedRecord.GSN] = startComputeTime
			}

			var startTransactionAnalysis time.Time
			if committedRecords[0].GSN > gsnThreshold {
				startTransactionAnalysis = time.Now()
			}
			AnalyzeTransaction(committedRecords, db)
			if committedRecords[0].GSN > gsnThreshold {
				transactionAnalysisLatencies += int(time.Since(startTransactionAnalysis).Nanoseconds())
			}

			// duration := time.Duration(2) * time.Millisecond
			// start := time.Now()
			// for time.Since(start) < duration {
			// 	// Busy-waiting
			// }

			// Iterate through committed records
			timestamp := time.Now().UnixNano()
			for _, record := range committedRecords {
				computeE2eEndTimes[record.GSN] = timestamp

				recordsReceived++
			}

			if committedRecords[0].GSN > gsnThreshold {
				batchesReceived++
				batchSize += len(committedRecords)
			}

			prevOffset = offset
		}
	}

	endThroughputTimer := time.Now().UnixNano()

	// Wait for everyone to finish their run
	time.Sleep(30 * time.Second)
	scalogApi.Stop <- true

	// Record records received
	recordsReceivedFilePath := "../../applications/vanilla_applications/transaction_analysis/data/records_received_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
	file, err := os.OpenFile(recordsReceivedFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	if _, err := file.WriteString(fmt.Sprintf("%d\n", recordsReceived)); err != nil {
		log.Fatal(err)
	}

	// Record transaction analysis latencies
	transactionAnalysisLatenciesFilePath := "../../applications/vanilla_applications/transaction_analysis/data/transaction_analysis_latencies_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
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
	computeE2eEndTimesFilePath := "../../applications/vanilla_applications/transaction_analysis/data/compute_e2e_end_times_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
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
	deliveryLatenciesFilePath := "../../applications/vanilla_applications/transaction_analysis/data/delivery_latencies_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
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
	startComputeTimesFilePath := "../../applications/vanilla_applications/transaction_analysis/data/start_compute_times_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
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
	batchSizesFilePath := "../../applications/vanilla_applications/transaction_analysis/data/batch_sizes_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
	file, err = os.OpenFile(batchSizesFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	if _, err := file.WriteString(fmt.Sprintf("%f\n", avgBatchSize)); err != nil {
		log.Fatal(err)
	}

	// Calculate throughput
	throughput := float64(recordsReceived) / float64((endThroughputTimer-startThroughputTimer)/1000000000)
	readThroughputFilePath := "../../applications/vanilla_applications/transaction_analysis/data/read_throughput_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
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
	fmt.Println("Running transaction analysis application")

	if len(os.Args) < 3 {
		fmt.Println("Usage: go run transaction_analysis.go <reader_id> <client_number>")
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

	TransactionAnalysisProcessing(int32(readerId), clientNumber)
}
