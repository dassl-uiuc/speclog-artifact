package main

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/boltdb/bolt"
	"github.com/scalog/scalog/benchmark/util"
	"github.com/scalog/scalog/client"
	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/scalog_api"
)

const NumberOfRequest = 10
const NumberOfBytes = 100
const StreamName = "AppendBenchmark"

const readType = "subscribe"

// global data structures to sync between readers and writers

var wg sync.WaitGroup

// time at which a record was computed on
var timeCompute map[int64]time.Time
var timeBeginCompute map[int64]time.Time
var timeRecompute map[int64]time.Time
var computationTimeUs int64

var runTimeSecs int

// global stop signal
var stop chan bool
var pause chan bool = make(chan bool)
var resume chan bool = make(chan bool)
var nextExpectedOffset int64 = 0
var confirmedGsn int64 = 0
var maxComputedGsn atomic.Int64 = atomic.Int64{}
var db *bolt.DB
var dbEntrySize = 2048
var padding = util.GenerateRandomString(dbEntrySize)

var gsnThreshold = int64(250000)

type UserState struct {
	AvgTransaction  int
	MaxTransaction  int
	MinTransaction  int
	NumTransactions int
}

type StateChange struct {
	UserId   int32
	OldState *UserState
	NewState *UserState
}

var StateChangesMutex sync.Mutex
var StateChanges map[int64]StateChange = make(map[int64]StateChange)

func GenerateRecord(length int) string {
	padding := util.GenerateRandomString(length - 4)
	rand.Seed(time.Now().UnixNano())
	// transacton amount
	transactionAmount := rand.Intn(9000) + 1000
	record := fmt.Sprintf("%04d%s", transactionAmount, padding)
	return record
}

func AnalyzeTransaction(committedRecords []client.CommittedRecord, db *bolt.DB) {
	err := db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("users"))
		if bucket == nil {
			log.Printf("Bucket 'users' does not exist")
			return nil
		}
		// Loop through the committed records and update each one
		for _, record := range committedRecords {
			if record.GSN > maxComputedGsn.Load() {
				maxComputedGsn.Store(record.GSN)
			}
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
				StateChangesMutex.Lock()
				StateChanges[record.GSN] = StateChange{
					UserId:   userID,
					OldState: nil,
					NewState: &UserState{AvgTransaction: transactionAmount, MaxTransaction: transactionAmount, MinTransaction: transactionAmount, NumTransactions: 1},
				}
				StateChangesMutex.Unlock()
				continue
			}
			existingValue := string(value)
			var avgTransaction, maxTransaction, minTransaction, numTransactions int
			_, err = fmt.Sscanf(existingValue, "%d,%d,%d,%d,", &avgTransaction, &maxTransaction, &minTransaction, &numTransactions)
			if err != nil {
				log.Printf("Failed to parse user data for user %s: %v", userIDStr, err)
				continue
			}
			oldState := UserState{AvgTransaction: avgTransaction, MaxTransaction: maxTransaction, MinTransaction: minTransaction, NumTransactions: numTransactions}
			totalTransactionAmount := float64(avgTransaction)*float64(numTransactions) + float64(transactionAmount)
			numTransactions++
			avgTransaction = int(totalTransactionAmount / float64(numTransactions))
			if transactionAmount > maxTransaction {
				maxTransaction = transactionAmount
			}
			if transactionAmount < minTransaction {
				minTransaction = transactionAmount
			}
			newState := UserState{AvgTransaction: avgTransaction, MaxTransaction: maxTransaction, MinTransaction: minTransaction, NumTransactions: numTransactions}
			userData := fmt.Sprintf("%d,%d,%d,%d,", avgTransaction, maxTransaction, minTransaction, numTransactions)
			userData = userData + padding[:dbEntrySize-len(userData)]
			err = bucket.Put(key, []byte(userData))
			if err != nil {
				log.Printf("Failed to update record for user %s: %v", userIDStr, err)
			}
			StateChangesMutex.Lock()
			StateChanges[record.GSN] = StateChange{
				UserId:   userID,
				OldState: &oldState,
				NewState: &newState,
			}
			StateChangesMutex.Unlock()
		}
		return nil
	})
	if err != nil {
		log.Printf("Failed to update database: %v", err)
	}
}

func appendThread(scalog *scalog_api.Scalog, id int, shardId int, timeSecs int, numUsers int) {
	defer wg.Done()
	numRecords := 0
	ticker := time.After(time.Duration(timeSecs) * time.Second)
	for {
		record := GenerateRecord(4096)
		recordId := rand.Intn(numUsers)
		err := scalog.FilterAppendToAssignedShard(int32(shardId), record, int32(recordId))
		if err != nil {
			goto end
		}

		if numRecords%1000 == 0 {
			log.Printf("[single_client_e2e]: client %v appended %v records", id, numRecords)
		}
		numRecords++
	end:
		select {
		case <-ticker:
			log.Printf("[single_client_e2e]: stopping client %v", id)
			scalog.StopAck <- true
			return
		default:
			continue
		}
	}
}

func CreateDatabase() *bolt.DB {
	dbFile := "/data/records.db"
	db, err := bolt.Open(dbFile, 0600, nil)
	if err != nil {
		log.Errorf("failed to open database: %v", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("users"))
		return err
	})
	if err != nil {
		log.Errorf("failed to create bucket: %v", err)
	}
	return db
}

func confirmationThread(c *scalog_api.Scalog) {
	for {
		select {
		case conf, ok := <-c.ConfC:
			if !ok {
				log.Printf("[confirmation_thread]: confirmation channel closed, returning")
				return
			}
			gsns := make([]int64, 0)
			for gsn := conf.StartGSN; gsn <= conf.EndGSN; gsn++ {
				gsns = append(gsns, gsn)
			}
			StateChangesMutex.Lock()
			for _, gsn := range gsns {
				if _, ok := StateChanges[gsn]; ok {
					delete(StateChanges, gsn)
				}
			}
			StateChangesMutex.Unlock()
		case misSpec := <-c.MisSpecC:
			log.Printf("misSpec %v", misSpec)
			if misSpec.Begin != 0 {
				// pause the computation thread
				pause <- true
				// update to holes
				c.UpdateToHoles(misSpec, misSpec.FailedShards)
				// iterate over unconfirmed state and print the gsns
				updatedGsns := make([]int64, 0)
				StateChangesMutex.Lock()
				db.Update(func(tx *bolt.Tx) error {
					bucket := tx.Bucket([]byte("users"))
					if bucket == nil {
						log.Printf("Bucket 'users' does not exist")
						return nil
					}
					for gsn := maxComputedGsn.Load(); gsn >= misSpec.Begin; gsn-- {
						if _, ok := StateChanges[gsn]; ok {
							// undo state change
							userID := StateChanges[gsn].UserId
							oldState := StateChanges[gsn].OldState
							userData := fmt.Sprintf("%d,%d,%d,%d,", oldState.AvgTransaction, oldState.MaxTransaction, oldState.MinTransaction, oldState.NumTransactions)
							userData = userData + padding[:dbEntrySize-len(userData)]
							key := []byte(fmt.Sprintf("%d", userID))
							err := bucket.Put(key, []byte(userData))
							if err != nil {
								log.Printf("Failed to update record for user %v: %v", userID, err)
							}
							updatedGsns = append(updatedGsns, gsn)
						}
					}
					return nil
				})
				StateChangesMutex.Unlock()
				timeStamp := time.Now()
				for _, gsn := range updatedGsns {
					timeRecompute[gsn] = timeStamp
				}
				// resume the computation thread
				nextExpectedOffset = c.FindFirstRealRecordAfterGsn(misSpec.Begin)
				resume <- true
			}
		default:
			continue
		}
	}
}

func computationThread(c *scalog_api.Scalog) {
	// Create database
	go confirmationThread(c)
	db = CreateDatabase()
	totalBatchSize := float64(0)
	numBatches := float64(0)
	printTicker := time.NewTicker(1 * time.Second)
	for {
		latestOffset := c.GetLatestOffset()
		latestOffset -= 1
		if latestOffset >= nextExpectedOffset {
			start := time.Now()
			lenBatch := latestOffset - nextExpectedOffset + 1
			committedRecords := make([]client.CommittedRecord, 0)
			for i := nextExpectedOffset; i <= latestOffset; i++ {
				record := c.Read(i)
				if record == (client.CommittedRecord{}) {
					log.Printf("skipping over failure hole: %v", c.Read(i).GSN)
					continue
				}
				committedRecords = append(committedRecords, record)
				timeBeginCompute[record.GSN] = start
			}
			AnalyzeTransaction(committedRecords, db)
			for i := nextExpectedOffset; i <= latestOffset; i++ {
				record := c.Read(i)
				if record == (client.CommittedRecord{}) {
					continue
				}
				timeCompute[c.Read(i).GSN] = time.Now()
			}
			totalBatchSize += float64(lenBatch)
			numBatches++

			nextExpectedOffset = latestOffset + 1
		}
		select {
		case <-printTicker.C:
			log.Printf("[single_client_e2e]: computed %v records", totalBatchSize)
			log.Printf("[single_client_e2e]: average batch size %v", totalBatchSize/numBatches)
		case <-stop:
			log.Printf("[single_client_e2e]: consumer thread terminating")
			c.Stop <- true
			return
		case <-pause:
			done := false
			for !done {
				select {
				case <-resume:
					done = true
				case <-stop:
					log.Printf("[single_client_e2e]: consumer thread terminating")
					c.Stop <- true
					return
				}
			}
			log.Printf("[single_client_e2e]: consumer thread resumed from nextExpectedOffset %v", nextExpectedOffset)
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
	// initialize global data structures
	timeCompute = make(map[int64]time.Time, 30000)
	timeBeginCompute = make(map[int64]time.Time, 30000)
	timeRecompute = make(map[int64]time.Time, 30000)
	stop = make(chan bool)
	computationTime, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Errorf("[single_client_e2e]: unable to parse time duration")
	}
	computationTimeUs = int64(computationTime)

	runTimeSecs, err = strconv.Atoi(os.Args[2])
	if err != nil {
		log.Errorf("[single_client_e2e]: unable to parse time duration")
	}

	shardId, err := strconv.Atoi(os.Args[3])
	if err != nil {
		log.Errorf("[single_client_e2e]: unable to parse shard id")
	}

	numAppenders, err := strconv.Atoi(os.Args[4])
	if err != nil {
		log.Errorf("[single_client_e2e]: unable to parse number of appenders")
	}

	filepath := os.Args[5]

	numUsers, err := strconv.Atoi(os.Args[6])
	if err != nil {
		log.Errorf("[single_client_e2e]: unable to parse number of users")
	}

	log.Printf("[single_client_e2e]: starting benchmark with runtime %v", runTimeSecs)
	var consumer *scalog_api.Scalog
	// start consumer
	for i := 0; i < 1; i++ {
		// client that relays confirmations to the consumer
		consumer = scalog_api.CreateClientWithRelay(0, -1, "../../.scalog.yaml")
		consumer.Subscribe(0)
		go computationThread(consumer)
	}

	appendClients := make([]*scalog_api.Scalog, 0)
	// start producer
	wg.Add(numAppenders)
	for i := 0; i < numAppenders; i++ {
		c := scalog_api.CreateClient(1000, shardId, "../../.scalog.yaml")
		go appendThread(c, i, i%4, runTimeSecs, numUsers)
		appendClients = append(appendClients, c)
	}
	wg.Wait()

	// stop computation thread
	stop <- true

	// append latencies
	appendLatencies := make([]LatencyTuple, 0)
	appendStartTimeMap := make(map[int64]time.Time)

	e2eMetrics, err := os.Create(filepath + "e2e_metrics_" + strconv.Itoa(shardId) + ".csv")
	if err != nil {
		log.Errorf("[single_client_e2e]: failed to open csv file")
	}
	defer e2eMetrics.Close()

	appendMetrics, err := os.Create(filepath + "append_metrics_" + strconv.Itoa(shardId) + ".csv")
	if err != nil {
		log.Errorf("[single_client_e2e]: failed to open csv file")
	}
	defer appendMetrics.Close()

	appendTput := float64(0)
	for _, appendClient := range appendClients {
		for gsn, appendStartTime := range appendClient.Stats.AppendStartTime {
			appendEndTime, ok := appendClient.Stats.AppendEndTime[gsn]
			if ok {
				appendLatencies = append(appendLatencies, LatencyTuple{gsn, appendEndTime.Sub(appendStartTime).Microseconds()})
				appendStartTimeMap[gsn] = appendStartTime
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
	header := []string{"gsn", "latency (us)", "append tput (ops/sec)"}
	appendWriter.Write(header)
	for _, latency := range appendLatencies {
		appendWriter.Write([]string{strconv.Itoa(int(latency.GSN)), strconv.Itoa(int(latency.Latency)), strconv.FormatFloat(appendTput, 'f', -1, 64)})
	}
	appendWriter.Flush()

	e2eWriter := csv.NewWriter(e2eMetrics)
	header = []string{"gsn", "delivery latency (us)", "confirm latency (us)", "compute latency (us)", "e2e latency (us)", "queuing delay (us)", "recompute latency (us)", "start time"}
	e2eWriter.Write(header)
	for _, latency := range appendLatencies {
		confirmTime, inConfirm := consumer.Stats.ConfirmTime[latency.GSN]
		computeTime, inCompute := timeCompute[latency.GSN]
		deliveryTime, inDelivery := consumer.Stats.DeliveryTime[latency.GSN]
		if inConfirm && inCompute && inDelivery {
			recomputeTime, inRecompute := timeRecompute[latency.GSN]
			var recomputeLatStr string
			if inRecompute {
				recomputeLatStr = strconv.Itoa(int(recomputeTime.Sub(appendStartTimeMap[latency.GSN]).Microseconds()))
				log.Printf("recompute latency %v", recomputeLatStr)
			} else {
				recomputeLatStr = "0"
			}
			e2eWriter.Write([]string{
				strconv.Itoa(int(latency.GSN)),
				strconv.Itoa(int(deliveryTime.Sub(appendStartTimeMap[latency.GSN]).Microseconds())),
				strconv.Itoa(int(confirmTime.Sub(appendStartTimeMap[latency.GSN]).Microseconds())),
				strconv.Itoa(int(computeTime.Sub(appendStartTimeMap[latency.GSN]).Microseconds())),
				strconv.Itoa(int(max(confirmTime.Sub(appendStartTimeMap[latency.GSN]).Microseconds(), computeTime.Sub(appendStartTimeMap[latency.GSN]).Microseconds()))),
				strconv.Itoa(int(timeBeginCompute[latency.GSN].Sub(consumer.Stats.DeliveryTime[latency.GSN]).Microseconds())),
				recomputeLatStr,
				appendStartTimeMap[latency.GSN].Format("15:04:05.000000"),
			})
		} else {
			// these are failed appends
			if !(inConfirm && !inCompute && !inDelivery) {
				e2eWriter.Write([]string{
					strconv.Itoa(int(latency.GSN)),
					strconv.FormatBool(inConfirm),
					strconv.FormatBool(inCompute),
					strconv.FormatBool(inDelivery)})
			}
		}
	}
	e2eWriter.Flush()

	log.Printf("recompute times %v", timeRecompute)
	log.Printf("[single_client_e2e]: benchmark complete")
}
