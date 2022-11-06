package address

import "fmt"

type GeneralOrderAddr struct {
	ip string
	port uint16
}

func NewGeneralOrderAddr(ip string, port uint16) *GeneralOrderAddr {
	return &GeneralOrderAddr{ip, port}
}

func (s *GeneralOrderAddr) UpdateAddr(port uint16) {
	s.port = port
}

func (s *GeneralOrderAddr) Get() string {
	return fmt.Sprintf("%v:%v", s.ip, s.port)
}