package main

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/scalog/scalog/client"
	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/pkg/address"
	"github.com/spf13/viper"
)

const NumberOfRequest = 10
const NumberOfBytes = 100
const StreamName = "AppendBenchmark"

const readType = "subscribe"

var wg sync.WaitGroup

func appendThread(client *client.Client, id int) {
	// run loop for 10 secs
	defer wg.Done()
	numRecords := 0
	ticker := time.After(10 * time.Second)
	for {
		str := "record-" + strconv.Itoa(numRecords) + "-" + strconv.Itoa(id)
		_, _, err := client.AppendOne(str)
		if err != nil {
			log.Errorf("%v", err)
		}
		numRecords++
		select {
		case <-ticker:
			fmt.Println("Client ", id, " appended ", numRecords, " records")
			return
		default:
			continue
		}
	}
}

func subscribeThread(client *client.Client, id int) {
	defer wg.Done()
	stream, err := client.Subscribe(0)
	ticker := time.After(10 * time.Second)
	if err != nil {
		log.Errorf("%v", err)
	}
	consumed := 0
	for {
		select {
		case <-stream:
			consumed++
			continue
		case <-ticker:
			fmt.Println("Consumed ", consumed, " records")
			return
		}
	}
}

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

	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
	}

	wg.Add(4)
	for i := 0; i < 2; i++ {
		c, err := client.NewClient(dataAddr, discAddr, numReplica)
		if err != nil {
			log.Fatalf("%v", err)
		}
		go appendThread(c, i)
	}

	for i := 0; i < 2; i++ {
		c, err := client.NewClient(dataAddr, discAddr, numReplica)
		if err != nil {
			log.Fatalf("%v", err)
		}
		go subscribeThread(c, i)
	}

	wg.Wait()
}
