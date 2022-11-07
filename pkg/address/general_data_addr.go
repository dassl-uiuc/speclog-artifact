package address

import (
	"fmt"
	"github.com/spf13/viper"
)

type GeneralDataAddr struct {
	ipFmt	   string
	numReplica int32
	basePort   uint16
}

func NewGeneralDataAddr(ipFmt string, numReplica int32, basePort uint16, ) *GeneralDataAddr {
	return &GeneralDataAddr{ipFmt, numReplica, basePort}
}

func (s *GeneralDataAddr) UpdateBasePort(basePort uint16) {
	s.basePort = basePort
}

func (s *GeneralDataAddr) Get(sid, rid int32) string {
	port := s.basePort + uint16(sid*s.numReplica+rid)
	return fmt.Sprintf("%v:%v", viper.GetString(fmt.Sprintf(s.ipFmt, sid, rid)), port)
}