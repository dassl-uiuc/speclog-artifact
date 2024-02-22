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

func main() {
	numberOfRequest, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Errorf("number of request should be integer")
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

	startTime := time.Now()
	for i := 0; i < numberOfRequest; i++ {
		dataGenStartTime := time.Now()
		record := util.GenerateRandomString(numberOfBytes)
		dataGenEndTime := time.Now()
		runStartTime := time.Now()
		gsn, shard, err := cli.AppendOne(record)
		runEndTime := time.Now()

		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			continue
		}

		GSNs = append(GSNs, gsn)
		shardIds = append(shardIds, shard)
		dataGenTimes = append(dataGenTimes, dataGenEndTime.Sub(dataGenStartTime))
		runTimes = append(runTimes, runEndTime.Sub(runStartTime))

		// _, _ = fmt.Fprintf(os.Stdout, "Append result: { Gsn: %d, Shard: %d, Size: %v bytes }\n", gsn, shard, len(record))
	}
	endTime := time.Now()

	util.LogCsvFile(numberOfRequest, numberOfBytes*numberOfRequest, endTime.Sub(startTime), GSNs, shardIds, runTimes, dataGenTimes, fileName)
}
