package main

import (
	"fmt"
	"time"
	"os"
	"strconv"

	"github.com/scalog/scalog/scalog_api"
)

var produceRunTime = int64(60)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Please provide desired rate")
		return
	}

	rate, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Invalid rate. It should be a number.")
		return
	}

	Produce(int32(rate))

	fmt.Println("All tasks completed")
}

func Produce(rate int32) {
	scalog_client := scalog_api.CreateClient()

	recordsProduced := 0
	startTimeInSeconds := time.Now().Unix()
	startThroughputTime := time.Now().UnixNano()

	ticker := time.NewTicker(time.Second / time.Duration(50))
	defer ticker.Stop()

	for (time.Now().Unix() - startTimeInSeconds < (produceRunTime)) {
		select {
		case <-ticker.C:
			data := fmt.Sprintf("test_%d", recordsProduced)
			_ = scalog_client.Append(data)
			recordsProduced++
			// fmt.Println("Records produced: ", recordsProduced)
		}
	}
	endThroughputTime := time.Now().UnixNano()

	// Print throughput in num ops / s
	throughput := float64(recordsProduced) / (float64(endThroughputTime - startThroughputTime) / 1e9)
	fmt.Printf("Throughput: %v ops/s\n", throughput)
	// fmt.Printf("Produced %v records\n", recordsProduced)
}