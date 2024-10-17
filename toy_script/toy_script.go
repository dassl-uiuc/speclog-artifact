package main

import (
	"fmt"
	"time"
	"sync"
	"os"
	"strconv"

	"github.com/scalog/scalog/scalog_api"
)

var consumeRunTime = int64(80)
var produceRunTime = int64(60)

func main() {
	fmt.Println("Running toy script")

	if len(os.Args) < 2 {
		fmt.Println("Please provide desired rate")
		return
	}

	rate, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Invalid rate. It should be a number.")
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go Consume(&wg)
	go Produce(&wg, int32(rate))

	wg.Wait()

	fmt.Println("All tasks completed")
}

func Produce(wg *sync.WaitGroup, rate int32) {
	defer wg.Done()
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
	fmt.Printf("Produced %v records\n", recordsProduced)
}

func Consume(wg *sync.WaitGroup) {
	defer wg.Done()
	scalog_client := scalog_api.CreateClient()
	scalog_client.Subscribe()

	recordsConsumed := 0
	startTimeInSeconds := time.Now().Unix()
	prevOffset := int64(0)
	for (time.Now().Unix() - startTimeInSeconds < (consumeRunTime)) {
		offset := scalog_client.GetLatestOffset()

		if offset != prevOffset {
			for i := prevOffset; i < offset; i++ {
				_ = scalog_client.Read(i)
				recordsConsumed++
			}
			prevOffset = offset
		}
	}

	fmt.Printf("Consumed %v records\n", recordsConsumed)
}