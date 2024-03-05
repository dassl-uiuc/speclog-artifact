package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/scalog/scalog/benchmark/util"
	"github.com/scalog/scalog/client"
	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/pkg/address"
	"github.com/spf13/viper"
)

func main() {
	timeLimit, err := time.ParseDuration(os.Args[1])
	if err != nil {
		log.Errorf("unable to parse time duration")
	}
	numberOfBytes, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Errorf("number of bytes should be integer")
	}
	inputFileName := os.Args[3]
	outputFileName := os.Args[4]
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

	// populate gsn to ShardId map
	ifile, err := os.Open(inputFileName)
	if err != nil {
		fmt.Println("error opening file:", err)
		return
	}
	defer ifile.Close()
	var gsnToShardId []int32

	scanner := bufio.NewScanner(ifile)
	for scanner.Scan() {
		line := scanner.Text()
		num, err := strconv.Atoi(line)
		if err != nil {
			fmt.Println("invalid entry in input file")
			return
		}
		gsnToShardId = append(gsnToShardId, int32(num))
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("error reading file:", err)
		return
	}

	var GSNs []int64
	var shardIds []int32
	var runTimes []time.Duration

	randGen := rand.New(rand.NewSource(0))
	startTime := time.Now()
	numberOfRequest := 0
	for stay, timeout := true, time.After(timeLimit); stay; {
		index := randGen.Intn(len(gsnToShardId))
		runStartTime := time.Now()
		_, err := cli.Read(int64(index), int32(gsnToShardId[index]), int32(0))
		runEndTime := time.Now()

		if err != nil {
			goto end
		}

		GSNs = append(GSNs, int64(index))
		shardIds = append(shardIds, int32(gsnToShardId[index]))
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

	util.LogCsvFile(len(runTimes), numberOfRequest*numberOfBytes, endTime.Sub(startTime), GSNs, shardIds, runTimes, nil, outputFileName)
}
