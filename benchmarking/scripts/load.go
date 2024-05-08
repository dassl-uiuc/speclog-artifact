package main

import (
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/scalog/scalog/benchmark/util"
	"github.com/scalog/scalog/client"
	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/pkg/address"
	"github.com/spf13/viper"
)

func append(threadId int, numberOfBytes int, gsnToShardId *[]int32, numRequests int, cli *client.Client, wg *sync.WaitGroup) {
	fmt.Printf("thread %d starting\n", threadId)
	for i := 0; i < numRequests; {
		record := util.GenerateRandomString(numberOfBytes)
		gsn, shardId, err := cli.AppendOne(record)
		if err != nil {
			fmt.Printf("error during append in thread %d when executing request number %d\n", threadId, i)
			continue
		}
		(*gsnToShardId)[gsn] = shardId
		i++
	}
	fmt.Printf("thread %d done loading\n", threadId)
	wg.Done()
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	numberOfBytes, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Errorf("number of bytes should be integer")
	}
	loadRecords, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Errorf("number of records to be loaded must be an integer")
	}
	numThreads, err := strconv.Atoi(os.Args[3])
	if err != nil {
		log.Errorf("number of threads to use for loading must be an integer")
	}
	fileName := os.Args[4]
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

	var wg sync.WaitGroup
	var gsnToShardId []int32

	gsnToShardId = make([]int32, loadRecords)
	recordsPerThread := loadRecords / numThreads
	numRecords := loadRecords

	wg.Add(numThreads)
	for i := 0; i < numThreads; i++ {
		go append(i, numberOfBytes, &gsnToShardId, min(numRecords, recordsPerThread), cli, &wg)
		numRecords -= min(numRecords, recordsPerThread)
	}
	wg.Wait()

	fmt.Println("done loading")

	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
	}
	defer file.Close()

	for i := 0; i < loadRecords; i++ {
		file.WriteString(fmt.Sprintln(gsnToShardId[i]))
	}
	err = file.Sync()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
	}

	fmt.Println("done writing gsnToShardId map to", fileName)
}
