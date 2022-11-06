package client

import (
	"fmt"
	log "github.com/scalog/scalog/logger"
)

func Start() {
	it, err := NewIt()
	fmt.Println("client start")
	if err != nil {
		log.Fatalf("%v", err)
		return
	}
	it.Start()
}
