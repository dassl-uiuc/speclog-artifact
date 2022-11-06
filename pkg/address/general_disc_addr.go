package address

import "fmt"

type GeneralDiscAddr struct {
	ip string
	port uint16
}

func NewGeneralDiscAddr(ip string, port uint16) *GeneralDiscAddr {
	return &GeneralDiscAddr{ip, port}
}

func (s *GeneralDiscAddr) UpdateAddr(port uint16) {
	s.port = port
}

func (s *GeneralDiscAddr) Get() string {
	return fmt.Sprintf("%v:%v", s.ip, s.port)
}