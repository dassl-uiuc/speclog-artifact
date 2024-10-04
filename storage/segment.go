package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	LogMetaDataLength   = 4
	MetaDataEntryLength = 8
	HolePrefix          = "0xDEADBEEF"
)

type Segment struct {
	path    string
	closed  bool
	baseLSN int64
	baseGSN int64
	nextSSN int32
	logPos  int32
	logFile *os.File
	lsnMap  map[int32]int32
	gsnMap  map[int32]int32
	mapMu   sync.RWMutex
	segLen  int32

	t []byte // metadata: reuse this across the lifetime of the segment
	b []byte // record: reuse this across the lifetime of the segment
}

func NewSegment(path string, baseLSN int64, segLen int32) (*Segment, error) {
	var err error
	s := &Segment{path: path, closed: false, baseLSN: baseLSN, nextSSN: 0, logPos: 0, segLen: segLen}
	s.lsnMap = make(map[int32]int32)
	s.gsnMap = make(map[int32]int32)
	s.t = make([]byte, LogMetaDataLength)
	s.b = make([]byte, 1024*1024) // maximum record size is 1MB
	// create directory if not exist
	if _, err = os.Stat(path); os.IsNotExist(err) {
		err = os.MkdirAll(path, 0755)
		if err != nil {
			return nil, err
		}
	}
	s.logFile, err = os.Create(fmt.Sprintf("%v/%v.log", path, baseLSN))
	if err != nil {
		return nil, err
	}
	return s, nil
}

func RecoverSegment(path string, baseLSN int64) (*Segment, error) {
	// TODO check if lsn map and gsn map exist: if they do, load them
	s := &Segment{path: path, closed: false, baseLSN: baseLSN, nextSSN: 0, logPos: 0}
	s.lsnMap = make(map[int32]int32)
	s.gsnMap = make(map[int32]int32)
	s.t = make([]byte, LogMetaDataLength)
	s.b = make([]byte, 1024*1024) // maximum record size is 1MB
	err := s.loadLog()
	if err != nil {
		return nil, err
	}
	// open the file in append mode to continue functioning
	s.logFile, err = os.OpenFile(fmt.Sprintf("%v/%v.log", s.path, baseLSN), os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// loadLog reads the log file and reconstruct content for ssnFile and gsnFile
func (s *Segment) loadLog() error {
	s.mapMu.Lock()
	defer s.mapMu.Unlock()
	// if gsn file exists, load it
	gsnPath := fmt.Sprintf("%v/%v.gsn", s.path, s.baseLSN)
	if _, err := os.Stat(gsnPath); err == nil {
		b := make([]byte, MetaDataEntryLength) //
		file, err := os.OpenFile(fmt.Sprintf("%v/%v.gsn", s.path, s.baseLSN), os.O_RDONLY, 0644)
		if err != nil {
			return err
		}
		defer file.Close()
		l, err := file.Read(b)
		if err != nil {
			return fmt.Errorf("Read base gsn error: %v", err)
		}
		if l != MetaDataEntryLength {
			return fmt.Errorf("Read base gsn error: expect length %v, get %v", MetaDataEntryLength, l)
		}
		s.baseGSN = int64(binary.LittleEndian.Uint64(b[:8]))
		for {
			l, err := file.Read(b)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			if l != MetaDataEntryLength {
				return fmt.Errorf("Read length error: expect %v get %v", MetaDataEntryLength, l)
			}
			gsn := int32(binary.LittleEndian.Uint32(b[:4]))
			pos := int32(binary.LittleEndian.Uint32(b[4:]))
			s.gsnMap[gsn] = pos
		}

	}
	// otherwise, reconstruct lsn file from the log
	file, err := os.OpenFile(fmt.Sprintf("%v/%v.log", s.path, s.baseLSN), os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	for {
		l, err := file.Read(s.t)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if l != LogMetaDataLength {
			return fmt.Errorf("Read log file %v error: expect length %v, get %v", s.baseLSN, LogMetaDataLength, l)
		}
		ll := binary.LittleEndian.Uint32(s.t)
		l, err = file.Read(s.b[:ll])
		if err == io.EOF {
			return fmt.Errorf("Unexpected EOF when reading log file %v error", s.baseLSN)
		}
		if err != nil {
			return err
		}
		if l != int(ll) {
			return fmt.Errorf("Read log file %v error: expect length %v, get %v", s.baseLSN, ll, l)
		}
		if strings.HasPrefix(string(s.b[:ll]), HolePrefix) {
			numHoles, err := strconv.ParseInt(string(s.b[len(HolePrefix):ll]), 10, 32)
			if err != nil {
				return fmt.Errorf("parse hole num error: %v", err)
			}
			for i := int32(0); i < int32(numHoles); i++ {
				s.lsnMap[s.nextSSN] = s.logPos
				s.nextSSN++
			}
		} else {
			s.lsnMap[s.nextSSN] = s.logPos
			s.nextSSN++
		}
		s.logPos += 4 + int32(ll)
	}
	return nil
}

func (s *Segment) Write(record string, holeSkip int32) (int32, error) {
	s.mapMu.Lock()
	defer s.mapMu.Unlock()
	if s.closed {
		return 0, fmt.Errorf("Segment closed")
	}
	if s.nextSSN >= s.segLen {
		return 0, fmt.Errorf("Segment full")
	}
	var err error
	ssn := s.nextSSN
	if holeSkip == 0 {
		fmt.Printf("%s: write record at ssn %d, pos %d\n", s.path, ssn, s.logPos)
		s.lsnMap[ssn] = s.logPos
		s.nextSSN++
	} else {
		fmt.Printf("%s: write %d holes\n", s.path, holeSkip)
		var i int32
		for i = int32(0); i < holeSkip; i++ {
			if s.nextSSN >= s.segLen {
				break // don't assign above the segment length
			}
			fmt.Printf("%s: ssn %d, pos %d\n", s.path, ssn+i, s.logPos)
			s.lsnMap[ssn+i] = s.logPos
			s.nextSSN++
		}
		record += strconv.Itoa(int(i))
	}
	// each record is structured as length+record
	l := int32(len(record))
	s.logPos += LogMetaDataLength + l

	binary.LittleEndian.PutUint32(s.t, uint32(l))
	_, err = s.logFile.Write(s.t)
	if err != nil {
		return 0, err
	}
	_, err = s.logFile.WriteString(record)
	if err != nil {
		return 0, err
	}
	return ssn, nil
}

func (s *Segment) Assign(ssn, length int32, gsn int64) error {
	s.mapMu.Lock()
	defer s.mapMu.Unlock()
	if s.closed {
		return fmt.Errorf("Segment closed")
	}
	if ssn == 0 {
		s.baseGSN = gsn
	}
	gsnOffset := int32(gsn - s.baseGSN)
	for i := int32(0); i < length; i++ {
		if pos, ok := s.lsnMap[ssn+i]; ok {
			s.gsnMap[gsnOffset+i] = pos
		} else {
			fmt.Printf("%s: no data in ssn=%v\n", s.path, ssn+i)
			fmt.Printf("%s: lsnMap:\n%v\n", s.path, s.lsnMap)
			os.Exit(1)
			// return fmt.Errorf("no data in ssn=%v", ssn+i)
		}
	}
	return nil
}

func writeMapToDisk(f string, m map[int32]int32, base int64) error {
	file, err := os.Create(f)
	if err != nil {
		return err
	}
	defer file.Close()
	// sort by keys
	keys := make([]int, len(m))
	i := 0
	for k := range m {
		keys[i] = int(k)
		i++
	}
	sort.Ints(keys)
	// write the map to file
	b := make([]byte, MetaDataEntryLength)
	binary.LittleEndian.PutUint64(b[0:8], uint64(base))
	_, err = file.Write(b)
	if err != nil {
		return err
	}
	for _, k := range keys {
		binary.LittleEndian.PutUint32(b[0:4], uint32(k))
		binary.LittleEndian.PutUint32(b[4:8], uint32(m[int32(k)]))
		_, err = file.Write(b)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Segment) Close() error {
	s.mapMu.Lock()
	defer s.mapMu.Unlock()
	if s.closed {
		return fmt.Errorf("Segment closed")
	}
	if len(s.lsnMap) != len(s.gsnMap) {
		return fmt.Errorf("Unable to close the segment: lsnMap size %v != gsnMap size %v", len(s.lsnMap), len(s.gsnMap))
	}
	err := s.logFile.Close()
	if err != nil {
		return err
	}
	err = writeMapToDisk(fmt.Sprintf("%v/%v.ssn", s.path, s.baseLSN), s.lsnMap, s.baseLSN)
	if err != nil {
		return err
	}
	err = writeMapToDisk(fmt.Sprintf("%v/%v.gsn", s.path, s.baseLSN), s.gsnMap, s.baseGSN)
	if err != nil {
		return err
	}
	s.closed = true
	return nil
}

func (s *Segment) Read(gsn int64) (string, error) {
	return s.ReadGSN(gsn)
}

func (s *Segment) ReadLSN(lsn int64) (string, error) {
	s.mapMu.RLock()
	pos := s.lsnMap[int32(lsn-s.baseLSN)]
	s.mapMu.RUnlock()
	return s.ReadPos(int64(pos))
}

func (s *Segment) ReadGSN(gsn int64) (string, error) {
	s.mapMu.RLock()
	pos, ok := s.gsnMap[int32(gsn-s.baseGSN)]
	s.mapMu.RUnlock()
	if ok {
		return s.ReadPos(int64(pos))
	}
	return "", fmt.Errorf("GSN %v doesn't exist", gsn)
}

func (s *Segment) ReadPos(pos int64) (string, error) {
	l, err := s.logFile.ReadAt(s.t, pos)
	if err != nil {
		return "", err
	}
	if l != LogMetaDataLength {
		return "", fmt.Errorf("Read log file %v error: expect length %v, get %v", s.baseLSN, LogMetaDataLength, l)
	}
	ll := binary.LittleEndian.Uint32(s.t)
	l, err = s.logFile.ReadAt(s.b[:ll], pos+4)
	if err != nil {
		return "", err
	}
	if l != int(ll) {
		return "", fmt.Errorf("Read log file %v error: expect length %v, get %v", s.baseLSN, ll, l)
	}
	return string(s.b[:ll]), nil
}
