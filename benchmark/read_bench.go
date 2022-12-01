package main

import (
	"fmt"
	"github.com/scalog/scalog/benchmark/util"
	"github.com/scalog/scalog/client"
	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/pkg/address"
	"github.com/spf13/viper"
	"os"
	"strconv"
	"time"
)

func main() {
	shardId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Errorf("shard id should be integer")
	}
	fileName := os.Args[2]
	// clean up old files
	err = os.RemoveAll("log")
	if err != nil {
		log.Errorf("%v", err)
	}
	// read configuration file
	viper.SetConfigFile("../.scalog.yaml")
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
	var runTimes []time.Duration

	totalBytes := 0

	startTime := time.Now()
	for gsn := 0; ; gsn++ {
		runStartTime := time.Now()
		record, err := cli.Read(int64(gsn), int32(shardId), int32(0))
		runEndTime := time.Now()

		if record == "" || err != nil {
			break
		}

		GSNs = append(GSNs, int64(gsn))
		shardIds = append(shardIds, int32(shardId))
		runTimes = append(runTimes, runEndTime.Sub(runStartTime))
		totalBytes += len(record)

		_, _ = fmt.Fprintf(os.Stdout, "Read Result: %v bytes\n", len(record))
		// _, _ = fmt.Fprintf(os.Stdout, "%v\n", record)
	}
	endTime := time.Now()

	util.LogCsvFile(len(runTimes), totalBytes, endTime.Sub(startTime), GSNs, shardIds, runTimes, nil, fileName)

}