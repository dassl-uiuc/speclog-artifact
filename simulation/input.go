package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:8888")
	if err != nil {
		fmt.Println("[simulation_client]: error connecting:", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	reader := bufio.NewReader(os.Stdin)
	for {
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("[simulation_client]: error reading from stdin:", err.Error())
			return
		}

		_, err = conn.Write([]byte(input))
		if err != nil {
			fmt.Println("[simulation_client]: error writing to connection:", err.Error())
			return
		}
	}
}
