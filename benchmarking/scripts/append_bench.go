package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/scalog/scalog/benchmark/util"
	"github.com/scalog/scalog/client"
	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/pkg/address"
	"github.com/spf13/viper"
	rateLimiter "golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

		// gsn, shard, err := cli.AppendOneTimeout(record, 1*time.Second)

		runEndTime := time.Now()

		if err != nil {
			if status.Code(err) == codes.DeadlineExceeded {
				fmt.Println("AppendOne call timed out, retrying...")
				continue
			}

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

func ack(cli *client.Client, runEndTimes *[]time.Time, stop chan bool) {
	for {
		select {
		case <-cli.AckC:
			*runEndTimes = append(*runEndTimes, time.Now())
		case <-stop:
			return
		}
	}
}

func appendStream(cli *client.Client, timeLimit time.Duration, numberOfBytes int, rate int, fileName string) {
	var GSNs []int64
	var shardIds []int32
	var dataGenTimes []time.Duration
	var runStartTimes []time.Time
	var runEndTimes []time.Time
	var runTimes []time.Duration
	var numberOfRequest int

	stop := make(chan bool)
	go ack(cli, &runEndTimes, stop)

	startTime := time.Now()
	numberOfRequest = 0

	fmt.Println("starting client with rate limit: ", rate)

	limiter := rateLimiter.NewLimiter(rateLimiter.Limit(rate), 1)

	for stay, timeout := true, time.After(timeLimit); stay; {
		err := limiter.Wait(context.Background())
		if err != nil {
			fmt.Errorf("rate limiter error: ", err)
			return
		}

		dataGenStartTime := time.Now()
		record := util.GenerateRandomString(numberOfBytes)
		dataGenEndTime := time.Now()

		var gsn int64
		var shard int32
		gsn, shard, err = cli.Append(record)
		runStartTimes = append(runStartTimes, time.Now())

		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			goto end
		}

		GSNs = append(GSNs, gsn)
		shardIds = append(shardIds, shard)
		dataGenTimes = append(dataGenTimes, dataGenEndTime.Sub(dataGenStartTime))
		numberOfRequest++

	end:
		select {
		case <-timeout:
			stay = false
		default:
		}
	}
	endTime := time.Now()

	close(stop)

	// Calculate difference between runEndTimes and runStartTimes
	for i := 0; i < len(runEndTimes); i++ {
		runTimes = append(runTimes, runEndTimes[i].Sub(runStartTimes[i]))
	}

	util.LogCsvFile(len(runEndTimes), numberOfBytes*numberOfRequest, endTime.Sub(startTime), GSNs, shardIds, runTimes, dataGenTimes, fileName)
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
	shardingHint, err := strconv.Atoi(os.Args[3])
	if err != nil {
		log.Errorf("sharding hint should be integer")
	}
	appendMode := os.Args[4]
	rate, err := strconv.Atoi(os.Args[5])
	if err != nil {
		log.Errorf("rate should be integer")
	}
	fileName := os.Args[6]

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

	cli, err := client.NewClientWithShardingHint(dataAddr, discAddr, numReplica, int64(shardingHint))
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
