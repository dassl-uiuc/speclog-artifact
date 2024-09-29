package main

import (
	"github.com/scalog/scalog/scalog_api"
	"fmt"
	"time"
	"os"
	"strconv"
)

var runTime = int64(115)

func Ping(appenderId int32) {
	scalogApi := scalog_api.CreateClient()

	recordsProduced := 0
	record := "Intrusion detected from: " + string(appenderId)
	startTimeInSeconds := time.Now().Unix()
	for (time.Now().Unix() - startTimeInSeconds < runTime) {
		scalogApi.AppendToAssignedShard(appenderId, record)

		recordsProduced++
	}

	fmt.Println("Produced ", recordsProduced, " records")
}

func main() {
	fmt.Println("Running intruion detection generator")

	if len(os.Args) < 2 {
		fmt.Println("Please provide appender id")
		return
	}

	appenderId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Invalid reader id. It should be a number.")
		return
	}

	Ping(int32(appenderId))
}