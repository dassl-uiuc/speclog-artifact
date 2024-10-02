package storage

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func check(t *testing.T, err error) {
	if err != nil {
		t.Errorf("%v", err)
	}
}

func TestNewSegment(t *testing.T) {
	record := "test"
	s, err := NewSegment("tmp", 0, 10)
	if err != nil {
		t.Errorf("%v", err)
	}
	if s == nil {
		t.Errorf("Get nil segment on creating")
	}
	l, err := s.Write(record, 0)
	if err != nil {
		t.Errorf("%v", err)
	}
	if l != 0 {
		t.Errorf("Write error: expect ssn %v, get %v", len(record), l)
	}
	r, err := s.ReadLSN(0)
	if err != nil {
		t.Errorf("%v", err)
	}
	if r != record {
		t.Errorf("Read error: expect '%v', get '%v'", record, r)
	}
	err = s.Assign(0, 1, 100)
	if err != nil {
		t.Errorf("%v", err)
	}
	r, err = s.ReadGSN(100)
	if err != nil {
		t.Errorf("%v", err)
	}
	if r != record {
		t.Errorf("Read error: expect '%v', get '%v'", record, r)
	}
	err = s.Close()
	if err != nil {
		t.Errorf("%v", err)
	}

	s, err = RecoverSegment("tmp", 0)
	if err != nil {
		t.Errorf("%v", err)
	}
	if s == nil {
		t.Errorf("Get nil segment on recovery")
	}
	r, err = s.ReadLSN(0)
	if err != nil {
		t.Errorf("%v", err)
	}
	if r != record {
		t.Errorf("Read error: expect '%v', get '%v'", record, r)
	}
	r, err = s.ReadGSN(100)
	if err != nil {
		t.Errorf("%v", err)
	}
	if r != record {
		t.Errorf("Read error: expect '%v', get '%v'", record, r)
	}
	err = s.Close()
	if err != nil {
		t.Errorf("%v", err)
	}

	err = os.RemoveAll("tmp")
	if err != nil {
		t.Errorf("%v", err)
	}
}

func TestHoles(t *testing.T) {
	record := HolePrefix
	s, err := NewSegment("tmp", 0, 10)
	assert.Nil(t, err)
	assert.NotNil(t, s, "Get nil segment on creating")

	l, err := s.Write(record, 6)
	assert.Nil(t, err)
	assert.Equal(t, int32(0), l, "Write error: expect ssn 0, get %v", l)

	l, err = s.Write(record, 5)
	assert.Nil(t, err)
	assert.Equal(t, int32(6), l, "Write error: expect ssn 10, get %v", l)

	l, err = s.Write(record, 5)
	assert.NotNil(t, err)
	assert.Equal(t, int32(0), l, "Write error: expect ssn 0, get %v", l)

	assert.Nil(t, s.Assign(0, 10, 100))

	assert.Nil(t, s.Close())

	s, err = RecoverSegment("tmp", 0)
	assert.Nil(t, err)

	r, err := s.ReadGSN(100)
	assert.Nil(t, err)
	assert.True(t, strings.HasPrefix(r, HolePrefix), "Read error: expect '%v*', get '%v'", HolePrefix, r)

	assert.Nil(t, s.Close())
	assert.Nil(t, os.RemoveAll("tmp"))
}

func TestMix(t *testing.T) {
	s, err := NewSegment("tmp", 0, 10)
	assert.Nil(t, err)
	assert.NotNil(t, s, "Get nil segment on creating")

	record := "test"
	hole := HolePrefix

	l, err := s.Write(record, 0)
	assert.Nil(t, err)
	assert.Equal(t, int32(0), l, "Write error: expect ssn 0, get %v", l)

	l, err = s.Write(hole, 5)
	assert.Nil(t, err)
	assert.Equal(t, int32(1), l, "Write error: expect ssn 1, get %v", l)

	l, err = s.Write(record, 0)
	assert.Nil(t, err)
	assert.Equal(t, int32(6), l, "Write error: expect ssn 6, get %v", l)

	// test read LSN
	r, err := s.ReadLSN(0)
	assert.Nil(t, err)
	assert.Equal(t, record, r, "Read error: expect '%v', get '%v'", record, r)

	r, err = s.ReadLSN(3)
	assert.Nil(t, err)
	assert.True(t, strings.HasPrefix(r, HolePrefix), "Read error: expect '%v*', get '%v'", HolePrefix, r)

	r, err = s.ReadLSN(6)
	assert.Nil(t, err)
	assert.Equal(t, record, r, "Read error: expect '%v', get '%v'", record, r)

	// test read GSN
	assert.Nil(t, s.Assign(0, 7, 100))

	r, err = s.ReadGSN(100)
	assert.Nil(t, err)
	assert.Equal(t, record, r, "Read error: expect '%v', get '%v'", record, r)

	r, err = s.ReadGSN(103)
	assert.Nil(t, err)
	assert.True(t, strings.HasPrefix(r, HolePrefix), "Read error: expect '%v*', get '%v'", HolePrefix, r)

	r, err = s.ReadGSN(106)
	assert.Nil(t, err)
	assert.Equal(t, record, r, "Read error: expect '%v', get '%v'", record, r)

	assert.Nil(t, s.Close())

	// after recovery, test again
	s, err = RecoverSegment("tmp", 0)
	assert.Nil(t, err)

	// test read LSN
	r, err = s.ReadLSN(0)
	assert.Nil(t, err)
	assert.Equal(t, record, r, "Read error: expect '%v', get '%v'", record, r)

	r, err = s.ReadLSN(3)
	assert.Nil(t, err)
	assert.True(t, strings.HasPrefix(r, HolePrefix), "Read error: expect '%v*', get '%v'", HolePrefix, r)

	r, err = s.ReadLSN(6)
	assert.Nil(t, err)
	assert.Equal(t, record, r, "Read error: expect '%v', get '%v'", record, r)

	// test read GSN
	assert.Nil(t, s.Assign(0, 7, 100))

	r, err = s.ReadGSN(100)
	assert.Nil(t, err)
	assert.Equal(t, record, r, "Read error: expect '%v', get '%v'", record, r)

	r, err = s.ReadGSN(103)
	assert.Nil(t, err)
	assert.True(t, strings.HasPrefix(r, HolePrefix), "Read error: expect '%v*', get '%v'", HolePrefix, r)

	r, err = s.ReadGSN(106)
	assert.Nil(t, err)
	assert.Equal(t, record, r, "Read error: expect '%v', get '%v'", record, r)

	assert.Nil(t, s.Close())
	assert.Nil(t, os.RemoveAll("tmp"))
}
