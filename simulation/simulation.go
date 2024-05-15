package main

import (
	"bufio"
	"fmt"
	"math"
	"net"
	"os"
	"sync/atomic"
	"time"

	movingaverage "github.com/RobinUS2/golang-moving-average"
	"gonum.org/v1/gonum/stat/distuv"
)

var run bool = false
var per_shard_quota []int64
var per_shard_throughput []int64
var shard_cut []int64

var dist distuv.Normal

type SimulationParameters struct {
	num_shard_primaries          int
	initial_per_shard_throughput int64
	batchingIntervalMicros       int
}

func addNoise(tput float64) float64 {
	dist.Sigma = 0.2 * tput
	return dist.Rand() + tput
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func get_quota(tput float64, params *SimulationParameters) int64 {
	return int64(math.Ceil(tput * float64(params.batchingIntervalMicros) / 1.0e6))
}

func get_tput(quota int64, params *SimulationParameters) float64 {
	return float64(quota) * 1.0e6 / float64(params.batchingIntervalMicros)
}

func simulation(params *SimulationParameters) {
	per_shard_quota = make([]int64, params.num_shard_primaries)
	per_shard_throughput = make([]int64, params.num_shard_primaries)
	initial_quota := get_quota(float64(params.initial_per_shard_throughput), params)

	for i := 0; i < params.num_shard_primaries; i++ {
		per_shard_quota[i] = initial_quota
		per_shard_throughput[i] = params.initial_per_shard_throughput
	}
	fmt.Println("[simulation]: initial quota: ", per_shard_quota)
	fmt.Println("[simulation]: initial throughput: ", per_shard_throughput)

	shard_cut = make([]int64, params.num_shard_primaries)
	for i := 0; i < params.num_shard_primaries; i++ {
		shard_cut[i] = 0
	}

	ma := make([]*movingaverage.MovingAverage, params.num_shard_primaries)
	for i := 0; i < params.num_shard_primaries; i++ {
		ma[i] = movingaverage.New(10)
	}

	print_ticker := time.NewTicker(1 * time.Second)
	for run {
		select {
		case <-print_ticker.C:
			fmt.Println("[simulation]: global cut: ", shard_cut)
			fmt.Print("[simulation]: moving averages: [")
			for i := 0; i < params.num_shard_primaries; i++ {
				fmt.Printf("%v", ma[i].Avg())
				if i < params.num_shard_primaries-1 {
					fmt.Print(", ")
				} else {
					fmt.Println("]")
				}
			}
		default:
			for i := 0; i < params.num_shard_primaries; i++ {
				tput := addNoise(float64(atomic.LoadInt64(&per_shard_throughput[i])))
				curr_tput := get_tput(per_shard_quota[i], params)
				shard_cut[i] = per_shard_quota[i]
				ma[i].Add(tput)
				window_avg := ma[i].Avg()
				if window_avg > 1.2*curr_tput || window_avg < 0.8*curr_tput {
					per_shard_quota[i] = get_quota(tput, params)
				}
			}
			time.Sleep(time.Duration(params.batchingIntervalMicros) * time.Microsecond)
		}
	}
}

func main() {
	dist = distuv.Normal{
		Mu: 0,
	}
	params := SimulationParameters{
		num_shard_primaries:          5,
		initial_per_shard_throughput: 20000,
		batchingIntervalMicros:       100,
	}
	run = true
	go simulation(&params)

	ln, err := net.Listen("tcp", ":8888")
	if err != nil {
		fmt.Println("[simulation]: error listening:", err.Error())
		os.Exit(1)
	}
	defer ln.Close()

	conn, err := ln.Accept()
	if err != nil {
		fmt.Println("[simulation]: error accepting connection:", err.Error())
		return
	}

	for {
		reader := bufio.NewReader(conn)
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("[simulation]: error reading from connection:", err.Error())
			return
		}

		if input == "e\n" {
			run = false
			break
		}

		var shard_index int
		var delta int64
		_, err = fmt.Sscanf(input, "%d %d", &shard_index, &delta)
		if err != nil {
			fmt.Printf("error: %v\n", err)
		}
		new_tput := atomic.AddInt64(&per_shard_throughput[shard_index], delta)
		fmt.Printf("[simulation]: shard %v new throughput: %v\n", shard_index, new_tput)
	}

	conn.Close()
}
