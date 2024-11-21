package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/RobinUS2/golang-moving-average"
	"github.com/scalog/scalog/scalog_api"
	"github.com/scalog/scalog/client"
	"github.com/spf13/viper"
	"database/sql"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

var intrusionDetectionConfigFilePath = "../../applications/vanilla_applications/intrusion_detection/intrusion_detection_config.yaml"
var movingAverage = movingaverage.New(10)

// TODO: Add computation here
func HandleIntrusion(committedRecords []client.CommittedRecord, db *sql.DB) {
	var placeholders []string
	var values []interface{}

	for _, record := range committedRecords {
		// Extract first two chars of record.Record and convert to int
		temperature, err := strconv.Atoi(record.Record[:2])
		if err != nil {
			fmt.Println("Error converting temperature to int")
		}

		// Add temperature to moving average
		movingAverage.Add(float64(temperature))
		
		// Add to placeholder and values to input to DB later
		placeholders = append(placeholders, "(?)")
		values = append(values, record.Record)
	}

	fmt.Println("Length of placeholders: ", len(placeholders))
	fmt.Println("Length of values: ", len(values))

	// Get moving average
	average := movingAverage.Avg()
	if average > 90 {
		fmt.Println("Intrusion detected")
	}

	// Build the SQL query
	insertQuery := fmt.Sprintf("INSERT INTO records (temperature) VALUES %s", strings.Join(placeholders, ","))

	// Insert the rows
	_, err := db.Exec(insertQuery, values...)
	if err != nil {
		log.Fatalf("Failed to insert rows: %v", err)
	}
}

func CreateDatabase() *sql.DB {
	dbFile := "/data/records.db"
	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		log.Fatalf("Failed to open SQLite database: %v", err)
	}

	// Create table if it doesn't exist
	createTableQuery := `
	CREATE TABLE IF NOT EXISTS records (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		temperature TEXT NOT NULL
	);`
	_, err = db.Exec(createTableQuery)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	return db
}

func DeleteDatabase(db *sql.DB) {
	dbFile := "/data/records.db"
	err := os.Remove(dbFile)
	if err != nil {
		log.Fatalf("Failed to delete SQLite database: %v", err)
	}
}

func IntrusionDetectionProcessing(readerId int32, clientNumber int) {
	// Create database
	db := CreateDatabase()

	// read configuration file
	viper.SetConfigFile(intrusionDetectionConfigFilePath)
	viper.AutomaticEnv()
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println("read config file error: %v", err)
	}
	runTime := int64(viper.GetInt("consume-run-time"))

	scalogApi := scalog_api.CreateClient(1000, -1, "/proj/rasl-PG0/tshong/speclog/.scalog.yaml")

	scalogApi.SubscribeToAssignedShard(readerId, 0)

	// var record map[string]interface{}
	// Time used to keep track of the time to run wordcount
	startTimeInSeconds := time.Now().Unix()
	prevOffset := int64(0)
	recordsReceived := 0
	computeE2eEndTimes := make(map[int64]int64)

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

			for i := prevOffset; i < offset; i++ {
				committedRecord := scalogApi.Read(i)

				committedRecords = append(committedRecords, committedRecord)
			}

			fmt.Println("Length of committed records: ", len(committedRecords))
			fmt.Println("Expected number of records: ", offset-prevOffset)

			HandleIntrusion(committedRecords, db)

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

			prevOffset = offset
		}
	}

	endThroughputTimer := time.Now().UnixNano()

	// Delete database
	DeleteDatabase(db)

	// Wait for everyone to finish their run
	time.Sleep(30 * time.Second)
	scalogApi.Stop <- true

	// Record records received
	recordsReceivedFilePath := "../../applications/vanilla_applications/intrusion_detection/data/records_received_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
	file, err := os.OpenFile(recordsReceivedFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	if _, err := file.WriteString(fmt.Sprintf("%d\n", recordsReceived)); err != nil {
		log.Fatal(err)
	}

	// Dump compute e2e latencies
	computeE2eEndTimesFilePath := "../../applications/vanilla_applications/intrusion_detection/data/compute_e2e_end_times_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
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
	deliveryLatenciesFilePath := "../../applications/vanilla_applications/intrusion_detection/data/delivery_latencies_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
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

	// Dump confirm latencies with GSNs
	confirmLatenciesFilePath := "../../applications/vanilla_applications/intrusion_detection/data/confirm_latencies_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
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
	readThroughputFilePath := "../../applications/vanilla_applications/intrusion_detection/data/read_throughput_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
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
