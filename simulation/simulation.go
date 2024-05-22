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

type AvgStream struct {
	sum   float64
	count int64
}

func (sp *AvgStream) Add(num float64) {
	sp.sum += num
	sp.count++
}

func (sp *AvgStream) Avg() float64 {
	if sp.count == 0 {
		return 0
	}
	avg := sp.sum / float64(sp.count)
	return math.Round(avg*100) / 100
}

var run bool = false
var per_shard_quota []int64
var per_shard_entries_per_bi []int64
var per_shard_queue []chan (Entry)
var per_shard_batch_delay_avg []AvgStream
var per_shard_batch_delay_moving_avg []*movingaverage.MovingAverage
var shard_cut []int64
var dist distuv.Normal

type Entry struct {
	gen_iter int64
}

type SimulationParameters struct {
	num_shard_primaries    int
	initial_entries_per_bi int64
	batchingIntervalMicros int
}

func addNoise(entries int64) int64 {
	dist.Sigma = 0 //float64(entries)
	return int64(math.Round(float64(entries) + dist.Rand()))
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func simulation(params *SimulationParameters) {
	per_shard_quota = make([]int64, params.num_shard_primaries)
	per_shard_entries_per_bi = make([]int64, params.num_shard_primaries)
	per_shard_queue = make([]chan (Entry), params.num_shard_primaries)
	per_shard_batch_delay_avg = make([]AvgStream, params.num_shard_primaries)
	per_shard_batch_delay_moving_avg = make([]*movingaverage.MovingAverage, params.num_shard_primaries)
	initial_quota := params.initial_entries_per_bi

	for i := 0; i < params.num_shard_primaries; i++ {
		per_shard_quota[i] = initial_quota
		per_shard_entries_per_bi[i] = initial_quota
		per_shard_queue[i] = make(chan (Entry), 4096)
		per_shard_batch_delay_avg[i] = AvgStream{sum: 0, count: 0}
		per_shard_batch_delay_moving_avg[i] = movingaverage.New(10)
	}
	fmt.Println("[simulation]: initial quota: ", per_shard_quota)
	fmt.Println("[simulation]: initial entries received at shard per bi: ", per_shard_entries_per_bi)

	shard_cut = make([]int64, params.num_shard_primaries)
	for i := 0; i < params.num_shard_primaries; i++ {
		shard_cut[i] = 0
	}

	ma := make([]*movingaverage.MovingAverage, params.num_shard_primaries)
	for i := 0; i < params.num_shard_primaries; i++ {
		ma[i] = movingaverage.New(10)
	}

	print_ticker := time.NewTicker(1 * time.Second)
	iter := int64(0)
	for run {
		select {
		case <-print_ticker.C:
			fmt.Println("[simulation]: statistics")
			fmt.Println("\tglobal cut: ", shard_cut)
			fmt.Print("\tmoving averages of generated entries: [")
			for i := 0; i < params.num_shard_primaries; i++ {
				fmt.Printf("%v", ma[i].Avg())
				if i < params.num_shard_primaries-1 {
					fmt.Print(", ")
				} else {
					fmt.Println("]")
				}
			}
			fmt.Print("\tappend batching delay averages: [")
			for i := 0; i < params.num_shard_primaries; i++ {
				fmt.Printf("%v", per_shard_batch_delay_avg[i].Avg())
				if i < params.num_shard_primaries-1 {
					fmt.Print(", ")
				} else {
					fmt.Println("]")
				}
			}
			fmt.Print("\tappend batching delay moving average: [")
			for i := 0; i < params.num_shard_primaries; i++ {
				fmt.Printf("%v", per_shard_batch_delay_moving_avg[i].Avg())
				if i < params.num_shard_primaries-1 {
					fmt.Print(", ")
				} else {
					fmt.Println("]")
				}
			}
			fmt.Print("\tbuffer length: [")
			for i := 0; i < params.num_shard_primaries; i++ {
				fmt.Printf("%v", len(per_shard_queue[i]))
				if i < params.num_shard_primaries-1 {
					fmt.Print(", ")
				} else {
					fmt.Println("]")
				}
			}

		default:
			for i := 0; i < params.num_shard_primaries; i++ {
				entries_gen := addNoise(atomic.LoadInt64(&per_shard_entries_per_bi[i]))
				for j := int64(0); j < entries_gen; j++ {
					entry := Entry{
						gen_iter: iter,
					}
					per_shard_queue[i] <- entry
				}
				shard_cut[i] = per_shard_quota[i]
				for j := int64(0); j < per_shard_quota[i]; j++ {
					select {
					case entry := <-per_shard_queue[i]:
						per_shard_batch_delay_avg[i].Add(float64(iter - entry.gen_iter))
						per_shard_batch_delay_moving_avg[i].Add(float64(iter - entry.gen_iter))
					default:
						break
					}
				}
				ma[i].Add(float64(entries_gen))
				if ma[i].Avg() > 1.1*float64(per_shard_quota[i]) || ma[i].Avg() < 0.9*float64(per_shard_quota[i]) {
					per_shard_quota[i] = int64(math.Ceil(ma[i].Avg()))
				}
			}
			time.Sleep(time.Duration(params.batchingIntervalMicros) * time.Microsecond)
			iter++
		}
	}
}

func main() {
	dist = distuv.Normal{
		Mu: 0,
	}
	params := SimulationParameters{
		num_shard_primaries:    5,
		initial_entries_per_bi: 2,
		batchingIntervalMicros: 100,
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
		new_tput := atomic.AddInt64(&per_shard_entries_per_bi[shard_index], delta)
		fmt.Printf("[simulation]: shard %v new entries per bi: %v\n", shard_index, new_tput)
	}

	conn.Close()
}
