package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/scalog/scalog/client"
	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/pkg/address"
	"github.com/spf13/viper"
)

const NumberOfRequest = 10
const NumberOfBytes = 100
const StreamName = "AppendBenchmark"

const readType = "subscribe"

func main() {
	var err error
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

	clients := make([]*client.Client, 3)
	clients[0], err = client.NewClient(dataAddr, discAddr, numReplica)
	clients[1], err = client.NewClient(dataAddr, discAddr, numReplica)
	clients[2], err = client.NewClient(dataAddr, discAddr, numReplica)

	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
	}

	var stream chan client.CommittedRecord
	if readType == "subscribe" {
		stream, err = clients[2].Subscribe(0)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}
	for numberOfRequest := 0; numberOfRequest < 3000; numberOfRequest++ {
		record := strconv.Itoa(numberOfRequest)
		gsn, _, err := clients[numberOfRequest%2].AppendOne(record)

		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			continue
		}
		fmt.Println("executing ", record, " ", gsn)
	}

	if readType != "subscribe" {
		for numberOfRequest := 0; numberOfRequest < 3000; numberOfRequest++ {
			str, err := clients[2].Read(int64(numberOfRequest), 0, 1)
			// fmt.Println(str)
			if err != nil {
				fmt.Fprintln(os.Stderr, "read failure")
			}
			num, _ := strconv.Atoi(str)
			if num != numberOfRequest {
				fmt.Println("bug!", num, " ", str, " ", numberOfRequest)
			}
		}
	} else {
		for i := 0; i < 3000; i++ {
			r := <-stream
			num, _ := strconv.Atoi(r.Record)
			if int64(num) != r.GSN {
				fmt.Println("bug!", num, " ", r.Record, " ", r.GSN)
			}
		}

	}
}
