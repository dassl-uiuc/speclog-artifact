package main

import (
	"github.com/scalog/scalog/scalog_api"
	"fmt"
	"time"
	"os"
	"strconv"
)

var runTime = int64(120)

func HandleIntrusion() {
	fmt.Println("Handling intrusion")
}

func IntrusionDetectionProcessing(readerId int32) {
	scalogApi := scalog_api.CreateClient()

	scalogApi.SubscribeToAssignedShard(readerId)

	// Time used to keep track of the time to run wordcount
	startTimeInSeconds := time.Now().Unix()
	prevOffset := int64(0)
	recordsReceived := 0
	for (time.Now().Unix() - startTimeInSeconds < (runTime)) {
		offset := scalogApi.GetLatestOffset()

		if offset != prevOffset {
			for i := prevOffset; i < offset; i++ {
				_ = scalogApi.Read(i)

				recordsReceived++

				// fmt.Println("Received record from reader ", readerId, ": ", record)

				HandleIntrusion()
			}

			prevOffset = offset
		}
	}

	fmt.Println("Received ", recordsReceived, " records")
}

func main() {
	fmt.Println("Running intrusion detection application")

	if len(os.Args) < 2 {
		fmt.Println("Please provide reader id")
		return
	}

	readerId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Invalid reader id. It should be a number.")
		return
	}

	IntrusionDetectionProcessing(int32(readerId))
}