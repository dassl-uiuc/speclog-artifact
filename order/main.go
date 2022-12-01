package order

import (
	"fmt"
	"net"
	"time"

	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/order/orderpb"

	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
)

func Start() {
	oid := int32(viper.GetInt("oid"))
	log.Infof("%v: %v", "oid", oid)
	StartOrder(oid)
}

func StartOrder(oid int32) {
	// read configuration
	numReplica := int32(viper.GetInt("order-replication-factor"))
	dataNumReplica := int32(viper.GetInt("data-replication-factor"))
	batchingInterval, err := time.ParseDuration(viper.GetString("order-batching-interval"))
	if err != nil {
		log.Fatalf("Failed to parse order-batching-interval: %v", err)
	}
	port := uint16(int32(viper.GetInt("order-port")) + oid)
	log.Infof("order-port: %v", port)
	raftPort := int32(viper.GetInt("raft-port"))
	ip := viper.GetString(fmt.Sprintf("order-%v-ip", oid))
	// generalOrderAddr := address.NewGeneralOrderAddr(ip, port)
	log.Infof("Starting order server %v at %v:%v", oid, ip, port)
	log.Infof("replication-factor: %v", numReplica)
	log.Infof("order-batching-interval: %v", batchingInterval)
	peerList := make([]string, numReplica)
	for i := int32(0); i < numReplica; i++ {
		peerIp := viper.GetString(fmt.Sprintf("order-%v-ip", i))
		peerList[int(i)] = fmt.Sprintf("http://%v:%v", peerIp, raftPort+i)
	}
	// listen to the port
	lis, err := net.Listen("tcp", fmt.Sprintf("%v:%v", ip, port)) //TODO
	if err != nil {
		log.Fatalf("Failed to listen to port %v: %v", port, err)
	}
	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,
		}),
	)
	// server should register all the services manually
	// use empty service name for all scalog services' health status,
	// see https://github.com/grpc/grpc/blob/master/doc/health-checking.md for more
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthgrpc.HealthCheckResponse_SERVING)
	healthgrpc.RegisterHealthServer(grpcServer, healthServer)
	// order server
	server := NewOrderServer(oid, numReplica, dataNumReplica, batchingInterval, peerList)
	orderpb.RegisterOrderServer(grpcServer, server)
	// serve grpc server
	go func() {
		err = grpcServer.Serve(lis)
		if err != nil {
			log.Fatalf("Failed to server grpc: %v", err)
		}
	}()
	server.Start()
	for {
		time.Sleep(time.Second)
	}
}
