package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/scalog/scalog/benchmark/util"
	"github.com/scalog/scalog/client"
	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/pkg/address"
	"github.com/spf13/viper"
)

const NumberOfRequest = 10
const NumberOfBytes = 100
const StreamName = "AppendBenchmark"

type tuple struct {
	gsn   int64
	shard int32
	err   error
}

func appendOne(cli *client.Client, timeLimit time.Duration, numberOfBytes int, fileName string) {
	var GSNs []int64
	var shardIds []int32
	var dataGenTimes []time.Duration
	var runTimes []time.Duration
	var numberOfRequest int

	startTime := time.Now()
	numberOfRequest = 0
	for stay, timeout := true, time.After(timeLimit); stay; {
		dataGenStartTime := time.Now()
		record := util.GenerateRandomString(numberOfBytes)
		dataGenEndTime := time.Now()
		runStartTime := time.Now()

		var gsn int64
		var shard int32
		gsn, shard, err := cli.AppendOne(record)
		runEndTime := time.Now()

		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			goto end
		}

		GSNs = append(GSNs, gsn)
		shardIds = append(shardIds, shard)
		dataGenTimes = append(dataGenTimes, dataGenEndTime.Sub(dataGenStartTime))
		runTimes = append(runTimes, runEndTime.Sub(runStartTime))
		numberOfRequest++

	end:
		select {
		case <-timeout:
			stay = false
		default:
		}
	}
	endTime := time.Now()

	util.LogCsvFile(numberOfRequest, numberOfBytes*numberOfRequest, endTime.Sub(startTime), GSNs, shardIds, runTimes, dataGenTimes, fileName)
}

func appendStream(cli *client.Client, timeLimit time.Duration, numberOfBytes int, rate int, fileName string) {
	go cli.ProcessAppend()

	var GSNs []int64
	var shardIds []int32
	var dataGenTimes []time.Duration
	var runStartTimes []time.Time
	var runEndTimes []time.Time
	var runTimes []time.Duration
	var numberOfRequest int

	startTime := time.Now()
	numberOfRequest = 0

	ticker := time.NewTicker(time.Second / time.Duration(rate))
	defer ticker.Stop()

	for stay, timeout := true, time.After(timeLimit); stay; {
		select {
		case <-ticker.C:
			dataGenStartTime := time.Now()
			record := util.GenerateRandomString(numberOfBytes)
			dataGenEndTime := time.Now()

			runStartTime := time.Now()
			runStartTimes = append(runStartTimes, runStartTime)

			var gsn int64
			var shard int32
			gsn, shard, err := cli.Append(record)

			if err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err)
				goto end
			}

			GSNs = append(GSNs, gsn)
			shardIds = append(shardIds, shard)
			dataGenTimes = append(dataGenTimes, dataGenEndTime.Sub(dataGenStartTime))
			numberOfRequest++
		}

	end:
		select {
		case <-timeout:
			stay = false
		default:
		}
	}
	endTime := time.Now()

	time.Sleep(60 * time.Second)

	runEndTimes = cli.GetRunEndTimes()
	if len(runEndTimes) != len(runStartTimes) {
		log.Errorf("runEndTimes and runStartTimes have different length")
		return
	}
	// Calculate difference between runEndTimes and runStartTimes
	for i := 0; i < len(runEndTimes); i++ {
		runTimes = append(runTimes, runEndTimes[i].Sub(runStartTimes[i]))
	}
	fmt.Println("run times lenght: ", len(runTimes))

	util.LogCsvFile(numberOfRequest, numberOfBytes*numberOfRequest, endTime.Sub(startTime), GSNs, shardIds, runTimes, dataGenTimes, fileName)
}

func main() {
	timeLimit, err := time.ParseDuration(os.Args[1])
	if err != nil {
		log.Errorf("unable to parse time duration")
	}
	numberOfBytes, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Errorf("number of bytes should be integer")
	}
	appendMode := os.Args[3]
	rate, err := strconv.Atoi(os.Args[4])
	if err != nil {
		log.Errorf("rate should be integer")
	}
	fileName := os.Args[5]

	// read configuration file
	viper.SetConfigFile("../../.scalog.yaml")
	viper.AutomaticEnv()
	err = viper.ReadInConfig()
	if err != nil {
		log.Errorf("read config file error: %v", err)
		return
	}

	numReplica := int32(viper.GetInt("data-replication-factor"))
	discPort := uint16(viper.GetInt("disc-port"))
	discIp := viper.GetString(fmt.Sprintf("disc-ip"))
	discAddr := address.NewGeneralDiscAddr(discIp, discPort)
	dataPort := uint16(viper.GetInt("data-port"))
	dataAddr := address.NewGeneralDataAddr("data-%v-%v-ip", numReplica, dataPort)

	cli, err := client.NewClient(dataAddr, discAddr, numReplica)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		return
	}

	if appendMode == "appendOne" {
		appendOne(cli, timeLimit, numberOfBytes, fileName)
	} else if appendMode == "append" {
		appendStream(cli, timeLimit, numberOfBytes, rate, fileName)
	} else {
		log.Errorf("invalid append mode")
		return
	}
}
