package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/scalog/scalog/benchmark/util"
	"github.com/scalog/scalog/scalog_api"

	// "sync"
	"github.com/spf13/viper"
)

var intrusionDetectionConfigFilePath = "../../applications/vanilla_applications/intrusion_detection/intrusion_detection_config.yaml"

func GenerateRecord(length int) string {
	padding := util.GenerateRandomString(length - 15)
	rand.Seed(time.Now().UnixNano())

	// Random temperature
	temperature := rand.Intn(90) + 10

	// Motion detection
	motionDetected := rand.Intn(2)

	// Random vibration
	vibration := float64(int(rand.Float64()*10)) / 10

	// Random light intensity
	lightIntensity := rand.Intn(90) + 10
	// Random sound intensity
	soundIntensity := rand.Intn(90) + 10

	// Random humidity
	humidity := rand.Intn(90) + 10

	// Random air pressure change
	airPressureChange := float64(int(rand.Float64()*10)) / 10

	// Add padding at the end
	// Ex: 1010.63040740.4
	record := fmt.Sprintf("%02d%d%.1f%d%d%d%.1f%s", temperature, motionDetected, vibration, lightIntensity, soundIntensity, humidity, airPressureChange, padding)
	return record
}

func Append_One_Ping(appenderId int32, clientNumber int) {
	// read configuration file
	viper.SetConfigFile(intrusionDetectionConfigFilePath)
	viper.AutomaticEnv()
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println("read config file error: %v", err)
	}
	runTime := int64(viper.GetInt("produce-run-time"))

	scalogApi := scalog_api.CreateClient(1000, -1, "../../speclog/.scalog.yaml")

	recordsProduced := 0
	startTimeInSeconds := time.Now().Unix()
	startThroughputTimer := time.Now().UnixNano()
	for time.Now().Unix()-startTimeInSeconds < runTime {
		record := GenerateRecord(4096)
		if err != nil {
			fmt.Println("Error marshalling record")
			return
		}

		scalogApi.AppendOneToAssignedShard(appenderId, record)

		recordsProduced++
	}

	// Calculate throughput
	endThroughputTimer := time.Now().UnixNano()

	WriteStats(recordsProduced, startThroughputTimer, endThroughputTimer, appenderId, clientNumber, scalogApi)
}

func Append_Stream_Ping(appenderId int32, clientNumber int) {
	// read configuration file
	viper.SetConfigFile(intrusionDetectionConfigFilePath)
	viper.AutomaticEnv()
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println("read config file error: %v", err)
	}
	runTime := int64(viper.GetInt("produce-run-time"))

	scalogApi := scalog_api.CreateClient(1000, -1, "../../.scalog.yaml")

	recordsProduced := 0
	startTimeInSeconds := time.Now().Unix()
	startThroughputTimer := time.Now().UnixNano()
	for time.Now().Unix()-startTimeInSeconds < runTime {
		record := GenerateRecord(4096)
		if err != nil {
			fmt.Println("Error marshalling record")
			return
		}

		scalogApi.AppendToAssignedShard(appenderId, record)

		recordsProduced++
	}

	// Calculate throughput
	endThroughputTimer := time.Now().UnixNano()

	scalogApi.StopAck <- true
	WriteStats(recordsProduced, startThroughputTimer, endThroughputTimer, appenderId, clientNumber, scalogApi)
}

func WriteStats(recordsProduced int, startThroughputTimer int64, endThroughputTimer int64, appenderId int32, clientNumber int, scalogApi *scalog_api.Scalog) {
	// Wait for everyone to finish their run
	time.Sleep(30 * time.Second)

	throughput := float64(recordsProduced) / float64((endThroughputTimer-startThroughputTimer)/1000000000)
	appendThroughputFilePath := "../../applications/vanilla_applications/intrusion_detection/data/append_throughput_" + strconv.Itoa(int(appenderId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
	file, err := os.OpenFile(appendThroughputFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// write the throughput value
	if _, err := file.WriteString(fmt.Sprintf("%f\n", throughput)); err != nil {
		log.Fatal(err)
	}

	// Record records produced
	appendRecordsProducedFilePath := "../../applications/vanilla_applications/intrusion_detection/data/append_records_produced_" + strconv.Itoa(int(appenderId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
	file, err = os.OpenFile(appendRecordsProducedFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// write the records produced
	if _, err := file.WriteString(fmt.Sprintf("%d\n", recordsProduced)); err != nil {
		log.Fatal(err)
	}

	appendStartTimestampsFilePath := "../../applications/vanilla_applications/intrusion_detection/data/append_start_timestamps_" + strconv.Itoa(int(appenderId)) + "_" + strconv.Itoa(clientNumber) + ".txt"
	file, err = os.OpenFile(appendStartTimestampsFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	appendStartTimes := scalogApi.Stats.AppendStartTime
	for gsn, time := range appendStartTimes {
		if _, err := file.WriteString(fmt.Sprintf("%d,%d\n", gsn, time.UnixNano())); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("Produced ", recordsProduced, " records")
}

func main() {
	fmt.Println("Running intrusion detection generator")

	if len(os.Args) < 4 {
		fmt.Println("Usage: go run intrusion_detection_generator.go <appender_id> <append_type> <client_number>")
		return
	}

	appenderId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Invalid reader id. It should be a number.")
		return
	}

	appendType, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Println("Invalid append type. It should be a number.")
		return
	}

	clientNumber, err := strconv.Atoi(os.Args[3])
	if err != nil {
		fmt.Println("Invalid client number. It should be a number.")
		return
	}

	if appendType == 0 {
		Append_One_Ping(int32(appenderId), clientNumber)
	} else {
		Append_Stream_Ping(int32(appenderId), clientNumber)
	}
}