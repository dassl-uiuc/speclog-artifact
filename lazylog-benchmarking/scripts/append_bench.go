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

var numTimeouts int64

func AppendOneWithTimeout(cli *client.Client, record string) (int64, int32, error) {
	for {
		channel := make(chan tuple, 1)
		go func() {
			g, s, e := cli.AppendOne(record)
			channel <- tuple{g, s, e}
		}()

		select {
		case res := <-channel:
			return res.gsn, res.shard, res.err
		case <-time.After(100 * time.Millisecond):
			numTimeouts = numTimeouts + 1
			continue
		}
	}
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
	fileName := os.Args[3]
	// clean up old files
	err = os.RemoveAll("log")
	if err != nil {
		log.Errorf("%v", err)
	}
	// read configuration file
	viper.SetConfigFile("../../.scalog.yaml")
	viper.AutomaticEnv()
	err = viper.ReadInConfig()
	if err != nil {
		log.Errorf("read config file error: %v", err)
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
	}

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
		gsn, shard, err := AppendOneWithTimeout(cli, record)
		runEndTime := time.Now()

		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			continue
		}

		GSNs = append(GSNs, gsn)
		shardIds = append(shardIds, shard)
		dataGenTimes = append(dataGenTimes, dataGenEndTime.Sub(dataGenStartTime))
		runTimes = append(runTimes, runEndTime.Sub(runStartTime))
		numberOfRequest++
		if numberOfRequest%500 == 0 {
			_, _ = fmt.Printf("executing %d\n", numberOfRequest)
		}
		select {
		case <-timeout:
			stay = false
		default:
		}
	}
	endTime := time.Now()

	util.LogCsvFile(numberOfRequest, numberOfBytes*numberOfRequest, endTime.Sub(startTime), GSNs, shardIds, runTimes, dataGenTimes, fileName, numTimeouts)
}
