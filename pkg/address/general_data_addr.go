package address

import "fmt"

type GeneralDataAddr struct {
	ip	       string
	numReplica int32
	basePort   uint16
}

func NewGeneralDataAddr(ip string, numReplica int32, basePort uint16) *GeneralDataAddr {
	return &GeneralDataAddr{ip, numReplica, basePort}
}

func (s *GeneralDataAddr) UpdateBasePort(basePort uint16) {
	s.basePort = basePort
}

func (s *GeneralDataAddr) Get(sid, rid int32) string {
	port := s.basePort + uint16(sid*s.numReplica+rid)
	return fmt.Sprintf("%v:%v", s.ip, port)
}