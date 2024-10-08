package main

import (
	"fmt"
	"time"
	"sync"

	"github.com/scalog/scalog/scalog_api"
)

var consumeRunTime = int64(80)
var produceRunTime = int64(60)

func main() {
	fmt.Println("Running toy script")

	var wg sync.WaitGroup
	wg.Add(2)

	go Consume(&wg)
	go Produce(&wg)

	wg.Wait()

	fmt.Println("All tasks completed")
}

func Produce(wg *sync.WaitGroup) {
	defer wg.Done()
	scalog_client := scalog_api.CreateClient()

	recordsProduced := 0
	startTimeInSeconds := time.Now().Unix()
	for (time.Now().Unix() - startTimeInSeconds < (produceRunTime)) {
		data := fmt.Sprintf("test_%d", recordsProduced)
		_ = scalog_client.Append(data)
		recordsProduced++
		// fmt.Println("Records produced: ", recordsProduced)
	}

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