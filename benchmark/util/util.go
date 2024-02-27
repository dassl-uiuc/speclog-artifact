package util

import (
	"encoding/csv"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

const CharSet string = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
const CharSetLength = 26 + 26 + 10

var randomStringMap = map[int]string{}

func GenerateRandomString(length int) string {
	if _, ok := randomStringMap[length]; ok {
		return randomStringMap[length]
	}
	rs := ""
	for i := 0; i < length; i++ {
		idx := rand.Intn(CharSetLength)
		rs = rs + string(CharSet[idx])
	}
	randomStringMap[length] = rs
	return randomStringMap[length]
}

func LogCsvFile(numberOfRequest, totalByte int, totalTime time.Duration, GSNs []int64, shardIds []int32, runTimes, dataGenTimes []time.Duration, filename string, numTimeouts int64) {
	file, err := os.Create(filename)
	if err != nil {
		log.Println("failed to open csv file")
	}
	defer file.Close()

	csvWriter := csv.NewWriter(file)

	startRow := []string{"id", "gsn", "shardId", "latency(ns)", "dataGenTime(ns)", "totalBytes", "totalTime(ns)", "numTimeouts"}
	err = csvWriter.Write(startRow)
	if err != nil {
		log.Println("failed to write csv file")
	}

	for i := 0; i < numberOfRequest; i++ {
		gsn := GSNs[i]
		shardId := shardIds[i]
		latency := runTimes[i]
		var dataGenTime time.Duration
		if dataGenTimes != nil {
			dataGenTime = dataGenTimes[i]
		} else {
			dataGenTime = 0
		}

		row := []string{strconv.Itoa(i), strconv.Itoa(int(gsn)), strconv.Itoa(int(shardId)), strconv.Itoa(int(latency)), strconv.Itoa(int(dataGenTime)), strconv.Itoa(totalByte), strconv.Itoa(int(totalTime)), strconv.Itoa(int(numTimeouts))}
		err = csvWriter.Write(row)
		if err != nil {
			log.Println("failed to write csv file")
		}
	}

	csvWriter.Flush()
}
