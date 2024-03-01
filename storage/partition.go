package storage

import (
	"fmt"
	"sync"
)

type Partition struct {
	path          string
	nextLSN       int64
	segments      []*Segment
	activeSegment *Segment
	activebaseLSN int64
	segLen        int32
	segmentsMu    sync.RWMutex
}

func NewPartition(path string, segLen int32) (*Partition, error) {
	var err error
	p := &Partition{path: path, nextLSN: 0, activebaseLSN: 0}
	p.segments = make([]*Segment, 0)
	p.activeSegment, err = NewSegment(path, p.activebaseLSN)
	if err != nil {
		return nil, err
	}
	p.segLen = segLen
	return p, nil
}

func (p *Partition) Write(record string) (int64, error) {
	p.segmentsMu.Lock()
	defer p.segmentsMu.Unlock()
	lsn := p.nextLSN
	p.nextLSN++
	if p.activeSegment.nextSSN >= p.segLen {
		err := p.CreateSegment()
		if err != nil {
			return 0, err
		}
	}
	_, err := p.activeSegment.Write(record)
	return lsn, err
}

func (p *Partition) Read(gsn int64) (string, error) {
	p.segmentsMu.RLock()
	defer p.segmentsMu.RUnlock()
	return p.ReadGSN(gsn)
}

func (p *Partition) ReadGSN(gsn int64) (string, error) {
	p.segmentsMu.RLock()
	defer p.segmentsMu.RUnlock()
	if gsn >= p.activeSegment.baseGSN {
		return p.activeSegment.ReadGSN(gsn)
	}
	f := func(s *Segment) int64 {
		return s.baseGSN
	}
	s := binarySearch(p.segments, f, gsn)
	if s == nil {
		return "", fmt.Errorf("Segment with gsn %v not exist", gsn)
	}
	return s.ReadGSN(gsn)
}

func (p *Partition) ReadLSN(lsn int64) (string, error) {
	p.segmentsMu.RLock()
	defer p.segmentsMu.RUnlock()
	if lsn >= p.activeSegment.baseLSN {
		return p.activeSegment.ReadLSN(lsn)
	}
	f := func(s *Segment) int64 {
		return s.baseLSN
	}
	s := binarySearch(p.segments, f, lsn)
	return s.ReadLSN(lsn)
}

func binarySearch(segs []*Segment, get func(*Segment) int64, target int64) *Segment {
	if len(segs) == 0 {
		return nil
	}
	for i, s := range segs {
		if get(s) > target {
			return segs[i-1]
		}
	}
	return segs[len(segs)-1]
}

func binarySearchIndex(segs []*Segment, get func(*Segment) int64, target int64) int {
	// if len(segs) == 0 {
	// 	return -1
	// }
	// low, high, mid := 0, len(segs)-1, 0
	// for low < high {
	// 	mid = low + (high-low)/2
	// 	if get(segs[mid]) >= target {
	// 		high = mid
	// 	} else {
	// 		low = mid + 1
	// 	}
	// }
	// return low
	if len(segs) == 0 {
		return -1
	}
	for i, s := range segs {
		if get(s) > target {
			return i - 1
		}
	}
	return len(segs) - 1
}

func (p *Partition) CreateSegment() error {
	var err error
	p.activebaseLSN += int64(p.segLen)
	p.segments = append(p.segments, p.activeSegment)
	p.activeSegment, err = NewSegment(p.path, p.activebaseLSN)
	return err
}

func min(a int32, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

// example:
// p.assign(5, 6, 10)
// s[1].assign(1, 3, 10), s[2].assign(0, 3, 13)
// 0, 1, 2, 3 | 4, 5, 6, 7 | 8, 9, 10, 11 -> active()
func (p *Partition) Assign(lsn int64, length int32, gsn int64) error {
	// Author: shreesha00
	// Writes can push active segment, need to potentially assign to earlier segments as well
	p.segmentsMu.RLock()
	defer p.segmentsMu.RUnlock()

	if lsn >= p.activebaseLSN {
		return p.activeSegment.Assign(int32(lsn-p.activebaseLSN), length, gsn)
	}

	f := func(s *Segment) int64 {
		return s.baseLSN
	}

	i := binarySearchIndex(p.segments, f, lsn)
	for length > 0 {
		if i < len(p.segments) {
			ssn := int32(lsn - p.segments[i].baseLSN)
			consumed := min(length, p.segLen-ssn)
			err := p.segments[i].Assign(ssn, consumed, gsn)
			if err != nil {
				return err
			}
			length -= consumed
			lsn += int64(consumed)
			gsn += int64(consumed)
			i++
		} else {
			return p.activeSegment.Assign(int32(lsn-p.activebaseLSN), length, gsn)
		}
	}

	return nil
}
