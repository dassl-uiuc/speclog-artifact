package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/scalog/scalog/client"
	"github.com/scalog/scalog/scalog_api"
	"github.com/spf13/viper"

	_ "github.com/mattn/go-sqlite3"

	"gonum.org/v1/gonum/mat"

	bolt "go.etcd.io/bbolt"
)

var hftConfigFilePath = "../../applications/vanilla_applications/hft/hft_config.yaml"

// TODO: Add computation here
func int32ToBytes(value int32) []byte {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, value)
	if err != nil {
		log.Fatalf("Error packing int32: %v", err)
	}
	return buf.Bytes()
}

func matrixToBytes(m *mat.Dense) []byte {
	rows, cols := m.Dims()
	var buf bytes.Buffer
	for i := 0; i < rows; i++ {
		for j := 0; j < cols; j++ {
			binary.Write(&buf, binary.BigEndian, m.At(i, j))
		}
	}
	return buf.Bytes()
}

func setBoltDB(dataFile string) *bolt.DB {
	db, err := bolt.Open(dataFile, 0600, nil)
	if err != nil {
		log.Fatalf("Failed to open or create database file %s: %v", dataFile, err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("BenchmarkBucket"))
		return err
	})
	if err != nil {
		log.Fatalf("Failed to create bucket: %v", err)
	}

	return db
}

func SeriesMBatchOptimized(dataPointsX []float64, WFull *mat.Dense, prevM *mat.Dense) *mat.Dense {
	alpha := 0.9

	n := len(dataPointsX)
	X := mat.NewDense(n, 2, nil) // Create a matrix with n rows and 2 columns
	for i, x := range dataPointsX {
		X.Set(i, 0, 1.0)
		X.Set(i, 1, x)
	}

	// Copy the appropriate slice of WFull to W
	W := mat.NewDense(n, n, nil)
	W.Copy(WFull.Slice(0, n, 0, n))

	// Step 2: Compute X^T * W * X
	weightedSum := mat.NewDense(2, 2, nil)
	temp := mat.NewDense(n, 2, nil)
	temp.Mul(W, X)               // W * X
	weightedSum.Mul(X.T(), temp) // X^T * (W * X)

	// Step 3: Add decayed M_{t-1}
	scaledPrevM := mat.NewDense(2, 2, nil)
	scaledPrevM.Scale(math.Pow(alpha, float64(n)), prevM)

	Mt := mat.NewDense(2, 2, nil)
	Mt.Add(scaledPrevM, weightedSum)

	// fmt.Printf("Optimized Batch Updated M_t:\n%v\n", mat.Formatted(Mt, mat.Prefix(" ")))
	// fmt.Printf("Execution time: %.6f milliseconds\n\n", duration.Seconds()*1000)

	return Mt
}

func SeriesVBatchOptimized(dataPointsX []float64, dataPointsY []float64, prevV *mat.Dense, WFull *mat.Dense) *mat.Dense {
	alpha := 0.9

	n := len(dataPointsX)
	X := mat.NewDense(n, 2, nil)
	for i, x := range dataPointsX {
		X.Set(i, 0, 1.0)
		X.Set(i, 1, x)
	}

	// Create the Y matrix from dataPointsY
	Y := mat.NewDense(n, 1, nil)
	for i, y := range dataPointsY {
		Y.Set(i, 0, y)
	}

	// Copy the appropriate slice of WFull to W
	W := mat.NewDense(n, n, nil)
	W.Copy(WFull.Slice(0, n, 0, n))

	// Step 2: Compute X^T * W * Y (weighted sum)
	weightedSum := mat.NewDense(2, 1, nil)
	temp := mat.NewDense(n, 1, nil)
	temp.Mul(W, Y)               // W * Y
	weightedSum.Mul(X.T(), temp) // X^T * (W * Y)

	// Step 3: Apply decay (weighted sum) and add previous V
	scaledPrevV := mat.NewDense(2, 1, nil)
	scaledPrevV.Scale(math.Pow(alpha, float64(n)), prevV)

	Vt := mat.NewDense(2, 1, nil)
	Vt.Add(scaledPrevV, weightedSum)

	// fmt.Printf("Optimized Batch Updated V_t:\n%v\n", mat.Formatted(Vt, mat.Prefix(" ")))
	// fmt.Printf("Execution time: %.6f milliseconds\n\n", duration.Seconds()*1000)

	return Vt
}

func Compute(
	records1 []client.CommittedRecord, records2 []client.CommittedRecord,
	WFull *mat.Dense, prevM *mat.Dense, prevV *mat.Dense) {
	dataPointsX := make([]float64, len(records1))
	dataPointsY := make([]float64, len(records2))

	for i, record := range records1 {
		value, err := strconv.ParseFloat(record.Record[:7], 64)
		if err != nil {
			fmt.Printf("Error converting Record %v to float64: %v\n", record.Record, err)
			continue
		}
		dataPointsX[i] = value
	}

	for i, record := range records2 {
		value, err := strconv.ParseFloat(record.Record[:7], 64)
		if err != nil {
			fmt.Printf("Error converting Record %v to float64: %v\n", record.Record, err)
			continue
		}
		dataPointsY[i] = value
	}

	W := WFull
	n := len(records1)
	if n >= 20 {
		W = mat.NewDense(n, n, nil)
		for i := 0; i < n; i++ {
			W.Set(i, i, math.Pow(0.9, float64(20-i-1)))
		}
	}

	Mt := SeriesMBatchOptimized(dataPointsX, W, prevM)
	Vt := SeriesVBatchOptimized(dataPointsX, dataPointsY, prevV, W)

	prevM = Mt
	prevV = Vt
}

func writeToBoltDB(M *mat.Dense, V *mat.Dense, timestamp uint32, db *bolt.DB) {
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, timestamp)

	mBytes := matrixToBytes(M)
	vBytes := matrixToBytes(V)

	key[0] &= 0x7F
	err := db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("BenchmarkBucket"))
		if bucket == nil {
			return fmt.Errorf("Bucket not found")
		}
		return bucket.Put(key, mBytes)
	})
	if err != nil {
		log.Fatalf("Failed to insert key %d: %v", timestamp, err)
	}

	key[0] |= 0x80
	err = db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("BenchmarkBucket"))
		if bucket == nil {
			return fmt.Errorf("Bucket not found")
		}
		return bucket.Put(key, vBytes)
	})
	if err != nil {
		log.Fatalf("Failed to insert key %d: %v", timestamp, err)
	}
}

func HftProcessing(readerId int32, readerId2 int32, clientNumber int) {
	// read configuration file
	db := setBoltDB("/data/hft.db")

	viper.SetConfigFile(hftConfigFilePath)
	viper.AutomaticEnv()
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println("read config file error: %v", err)
	}
	runTime := int64(viper.GetInt("consume-run-time"))
	numReadClients := int32(viper.GetInt("num-append-clients"))

	fmt.Printf("runtime=%ds\n", runTime)

	M := mat.NewDense(2, 2, nil)
	V := mat.NewDense(2, 1, nil)
	WFull := mat.NewDense(20, 20, nil)
	for i := 0; i < 20; i++ {
		WFull.Set(i, i, math.Pow(0.9, float64(20-i-1)))
	}

	filterValue := numReadClients
	scalogApi := scalog_api.CreateClient(1000, -1, "/proj/rasl-PG0/JiyuHu23/speclog/.scalog.yaml")

	scalogApi.FilterSubscribeDouble(0, readerId, readerId2, filterValue)

	fmt.Printf("readerId: %d, readerId2: %d, filterValue: %d\n", readerId, readerId2, filterValue)

	// var record map[string]interface{}
	// Time used to keep track of the time to run wordcount
	startTimeInSeconds := time.Now().Unix()
	prevOffset := int64(0)
	computeE2eEndTimes := make(map[int64]int64)
	timeBeginCompute := make(map[int64]time.Time)
	batchesReceived := 0
	batchSize := 0
	diffSize := 0
	hftLatencies := 0

	// Wait for first record to come in before starting throughput timer
	for {
		offset := scalogApi.GetLatestOffset()
		if offset != prevOffset {
			break
		}
	}

	fmt.Println("Start processing")

	committedRecords1 := make([]client.CommittedRecord, 0)
	committedRecords2 := make([]client.CommittedRecord, 0)

	startThroughputTimer := time.Now().UnixNano()
	for time.Now().Unix()-startTimeInSeconds < (runTime) {
		offset := scalogApi.GetLatestOffset()
		if offset != prevOffset {
			startComputeTime := time.Now()
			for i := prevOffset; i < offset; i++ {
				committedRecord := scalogApi.Read(i)

				if committedRecord.RecordId%filterValue == readerId {
					committedRecords1 = append(committedRecords1, committedRecord)
				} else if committedRecord.RecordId%filterValue == readerId2 {
					committedRecords2 = append(committedRecords2, committedRecord)
				}
				timeBeginCompute[committedRecord.GSN] = startComputeTime
			}

			// fmt.Println("Length of committed records: ", len(committedRecords))
			// fmt.Println("Expected number of records: ", offset-prevOffset)

			// startHandleIntrusion := time.Now()
			// HandleIntrusion(committedRecords, db)
			// handleIntrusionLatencies += int(time.Since(startHandleIntrusion).Nanoseconds())

			currBatch := int(math.Min(float64(len(committedRecords1)), float64(len(committedRecords2))))
			if currBatch == 0 {
				continue
			}
			diffSize += int(math.Abs(float64(len(committedRecords1) - len(committedRecords2))))

			start := time.Now()

			// do computation and write to boltdb
			Compute(committedRecords1[:currBatch], committedRecords2[:currBatch], WFull, M, V)
			writeToBoltDB(M, V, uint32(batchesReceived), db)

			hftLatencies += int(time.Since(start).Nanoseconds())

			// Iterate through committed records
			timestamp := time.Now().UnixNano()
			for _, record := range committedRecords1 {
				computeE2eEndTimes[record.GSN] = timestamp
			}
			for _, record := range committedRecords2 {
				computeE2eEndTimes[record.GSN] = timestamp
			}

			batchesReceived++
			batchSize += currBatch

			prevOffset = offset

			// clear the slice
			committedRecords1 = committedRecords1[:0]
			committedRecords2 = committedRecords2[:0]
			// fmt.Println("processed a batch ", offset)
		}
	}

	fmt.Println("processed a batch ", scalogApi.GetLatestOffset())
	fmt.Printf("avg batch size %.2f, avg record len diff %.2f\n",
		float64(batchSize)/float64(batchesReceived), float64(diffSize)/float64(batchesReceived))
	fmt.Println(batchSize, batchesReceived)
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

	recordsReceived := len(computeE2eEndTimes)
	if _, err := file.WriteString(fmt.Sprintf("%d\n", recordsReceived)); err != nil {
		log.Fatal(err)
	}

	// Record transaction analysis latencies
	hftLatenciesFilePath := "../../applications/vanilla_applications/hft/data/hft_latencies_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
	file, err = os.OpenFile(hftLatenciesFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	averageHftLatencies := float64(hftLatencies) / float64(batchesReceived)
	if _, err := file.WriteString(fmt.Sprintf("%f\n", averageHftLatencies)); err != nil {
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

// func HftProcessing(readerId int32, readerId2 int32, clientNumber int) {
// 	// read configuration file
// 	viper.SetConfigFile(hftConfigFilePath)
// 	viper.AutomaticEnv()
// 	err := viper.ReadInConfig()
// 	if err != nil {
// 		fmt.Println("read config file error: %v", err)
// 	}
// 	runTime := int64(viper.GetInt("consume-run-time"))
// 	numReadClients := int32(viper.GetInt("num-append-clients"))

// 	fmt.Printf("runtime=%ds\n", runTime)

// 	filterValue := numReadClients
// 	scalogApi1 := scalog_api.CreateClient(1000, -1, "/proj/rasl-PG0/JiyuHu23/speclog/.scalog.yaml")
// 	scalogApi2 := scalog_api.CreateClient(1000, -1, "/proj/rasl-PG0/JiyuHu23/speclog/.scalog.yaml")

// 	scalogApi1.FilterSubscribe(0, readerId, filterValue)
// 	scalogApi2.FilterSubscribe(0, readerId2, filterValue)

// 	fmt.Printf("readerId: %d, readerId2: %d, filterValue: %d\n", readerId, readerId2, filterValue)

// 	// var record map[string]interface{}
// 	// Time used to keep track of the time to run wordcount
// 	startTimeInSeconds := time.Now().Unix()
// 	prevOffset1 := int64(0)
// 	prevOffset2 := int64(0)
// 	recordsReceived := 0
// 	computeE2eEndTimes := make(map[int64]int64)
// 	timeBeginCompute := make(map[int64]time.Time)
// 	batchesReceived := 0
// 	batchSize := 0
// 	transactionAnalysisLatencies := 0

// 	// Wait for first record to come in before starting throughput timer
// 	for {
// 		offset1 := scalogApi1.GetLatestOffset()
// 		offset2 := scalogApi2.GetLatestOffset()
// 		if offset1 != prevOffset1 && offset2 != prevOffset2 {
// 			break
// 		}
// 	}

// 	fmt.Println("Start processing")

// 	startThroughputTimer := time.Now().UnixNano()
// 	for time.Now().Unix()-startTimeInSeconds < (runTime) {
// 		offset1 := scalogApi1.GetLatestOffset()
// 		offset2 := scalogApi2.GetLatestOffset()

// 		if offset1 != prevOffset1 && offset2 != prevOffset2 {
// 			committedRecords1 := make([]client.CommittedRecord, 0)
// 			committedRecords2 := make([]client.CommittedRecord, 0)

// 			startComputeTime := time.Now()
// 			for i := prevOffset1; i < offset1; i++ {
// 				committedRecord := scalogApi1.Read(i)

// 				committedRecords1 = append(committedRecords1, committedRecord)

// 				timeBeginCompute[committedRecord.GSN] = startComputeTime
// 			}
// 			for i := prevOffset2; i < offset2; i++ {
// 				committedRecord := scalogApi2.Read(i)

// 				committedRecords2 = append(committedRecords2, committedRecord)

// 				timeBeginCompute[committedRecord.GSN] = startComputeTime
// 			}

// 			// fmt.Println("Length of committed records: ", len(committedRecords))
// 			// fmt.Println("Expected number of records: ", offset-prevOffset)

// 			// startHandleIntrusion := time.Now()
// 			// HandleIntrusion(committedRecords, db)
// 			// handleIntrusionLatencies += int(time.Since(startHandleIntrusion).Nanoseconds())

// 			duration := time.Duration(800) * time.Microsecond
// 			start := time.Now()
// 			for time.Since(start) < duration {
// 				// Busy-waiting
// 			}

// 			// Iterate through committed records
// 			timestamp := time.Now().UnixNano()
// 			for _, record := range committedRecords1 {
// 				computeE2eEndTimes[record.GSN] = timestamp

// 				recordsReceived++
// 			}
// 			for _, record := range committedRecords2 {
// 				computeE2eEndTimes[record.GSN] = timestamp

// 				recordsReceived++
// 			}

// 			batchesReceived++
// 			batchSize += int(math.Min(float64(len(committedRecords1)), float64(len(committedRecords2))))

// 			prevOffset1 = offset1
// 			prevOffset2 = offset2
// 			// fmt.Println("processed a batch ", offset)
// 		}
// 	}

// 	fmt.Println("processed a batch ", scalogApi1.GetLatestOffset())
// 	fmt.Printf("avg batch size %.2f\n", float64(batchSize)/float64(batchesReceived))
// 	fmt.Println(batchSize, batchesReceived)
// 	endThroughputTimer := time.Now().UnixNano()

// 	// Wait for everyone to finish their run
// 	time.Sleep(30 * time.Second)
// 	scalogApi1.Stop <- true
// 	scalogApi2.Stop <- true

// 	// Record records received
// 	recordsReceivedFilePath := "../../applications/vanilla_applications/hft/data/records_received_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
// 	file, err := os.OpenFile(recordsReceivedFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer file.Close()

// 	if _, err := file.WriteString(fmt.Sprintf("%d\n", recordsReceived)); err != nil {
// 		log.Fatal(err)
// 	}

// 	// Record transaction analysis latencies
// 	transactionAnalysisLatenciesFilePath := "../../applications/vanilla_applications/hft/data/hft_latencies_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
// 	file, err = os.OpenFile(transactionAnalysisLatenciesFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer file.Close()

// 	averageTransactionAnalysisLatencies := float64(transactionAnalysisLatencies) / float64(batchesReceived)
// 	if _, err := file.WriteString(fmt.Sprintf("%f\n", averageTransactionAnalysisLatencies)); err != nil {
// 		log.Fatal(err)
// 	}

// 	// Dump compute e2e latencies
// 	computeE2eEndTimesFilePath := "../../applications/vanilla_applications/hft/data/compute_e2e_end_times_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
// 	file, err = os.OpenFile(computeE2eEndTimesFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer file.Close()

// 	for gsn, time := range computeE2eEndTimes {
// 		if _, err := file.WriteString(fmt.Sprintf("%d,%d\n", gsn, time)); err != nil {
// 			log.Fatal(err)
// 		}
// 	}

// 	// Dump delivery latencies with GSNs
// 	deliveryLatenciesFilePath := "../../applications/vanilla_applications/hft/data/delivery_latencies_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
// 	file, err = os.OpenFile(deliveryLatenciesFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer file.Close()

// 	deliveryTimes := scalogApi1.Stats.DeliveryTime
// 	for gsn, time := range deliveryTimes {
// 		if _, err := file.WriteString(fmt.Sprintf("%d,%d\n", gsn, time.UnixNano())); err != nil {
// 			log.Fatal(err)
// 		}
// 	}
// 	deliveryTimes = scalogApi2.Stats.DeliveryTime
// 	for gsn, time := range deliveryTimes {
// 		if _, err := file.WriteString(fmt.Sprintf("%d,%d\n", gsn, time.UnixNano())); err != nil {
// 			log.Fatal(err)
// 		}
// 	}

// 	// Dump start compute times
// 	startComputeTimesFilePath := "../../applications/vanilla_applications/hft/data/start_compute_times_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
// 	file, err = os.OpenFile(startComputeTimesFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer file.Close()

// 	for gsn, time := range timeBeginCompute {
// 		if _, err := file.WriteString(fmt.Sprintf("%d,%d\n", gsn, time.UnixNano())); err != nil {
// 			log.Fatal(err)
// 		}
// 	}

// 	// Dump batch sizes
// 	avgBatchSize := float64(batchSize) / float64(batchesReceived)
// 	batchSizesFilePath := "../../applications/vanilla_applications/hft/data/batch_sizes_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
// 	file, err = os.OpenFile(batchSizesFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer file.Close()

// 	if _, err := file.WriteString(fmt.Sprintf("%f\n", avgBatchSize)); err != nil {
// 		log.Fatal(err)
// 	}

// 	// Dump confirm latencies with GSNs
// 	confirmLatenciesFilePath := "../../applications/vanilla_applications/hft/data/confirm_latencies_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
// 	file, err = os.OpenFile(confirmLatenciesFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer file.Close()

// 	confirmTimes := scalogApi1.Stats.ConfirmTime
// 	for gsn, time := range confirmTimes {
// 		if _, err := file.WriteString(fmt.Sprintf("%d,%d\n", gsn, time.UnixNano())); err != nil {
// 			log.Fatal(err)
// 		}
// 	}
// 	confirmTimes = scalogApi2.Stats.ConfirmTime
// 	for gsn, time := range confirmTimes {
// 		if _, err := file.WriteString(fmt.Sprintf("%d,%d\n", gsn, time.UnixNano())); err != nil {
// 			log.Fatal(err)
// 		}
// 	}

// 	// Calculate throughput
// 	throughput := float64(recordsReceived) / float64((endThroughputTimer-startThroughputTimer)/1000000000)
// 	readThroughputFilePath := "../../applications/vanilla_applications/hft/data/read_throughput_" + strconv.Itoa(int(readerId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
// 	file, err = os.OpenFile(readThroughputFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer file.Close()

// 	// write the throughput value
// 	if _, err := file.WriteString(fmt.Sprintf("%f\n", throughput)); err != nil {
// 		log.Fatal(err)
// 	}

// 	// Purpose is to wait for producers to finish appending and write their stats to files
// 	endingTimeout := 10
// 	startTimeInSeconds = time.Now().Unix()
// 	for time.Now().Unix()-startTimeInSeconds < (int64(endingTimeout)) {
// 		// wait for the remaining time to elapse
// 	}

// 	fmt.Println("Received ", recordsReceived, " records")
// }

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
