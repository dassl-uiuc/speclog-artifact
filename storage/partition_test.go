package storage

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPartition(t *testing.T) {
	record := "test"
	p, err := NewPartition("tmp", 1000)
	check(t, err)
	if p == nil {
		t.Errorf("Get nil segment on creating")
	}
	l, err := p.Write(record, 0)
	check(t, err)
	if l != 0 {
		t.Errorf("Write error: expect ssn %v, get %v", len(record), l)
	}
	r, err := p.ReadLSN(0)
	check(t, err)
	if r != record {
		t.Errorf("Read error: expect '%v', get '%v'", record, r)
	}
	err = p.Assign(0, 1, 100)
	check(t, err)

	r, err = p.ReadGSN(100)
	check(t, err)
	if r != record {
		t.Errorf("Read error: expect '%v', get '%v'", record, r)
	}

	err = p.Close()
	check(t, err)

	err = os.RemoveAll("tmp")
	check(t, err)
}

func TestWriteAcrossSeg(t *testing.T) {
	p, err := NewPartition("tmp", 10)
	assert.Nil(t, err)
	assert.NotNil(t, p)

	hole := HolePrefix

	l, err := p.Write(hole, 8)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), l)

	l, err = p.Write(hole, 8)
	assert.Nil(t, err)
	assert.Equal(t, int64(8), l)

	r, err := p.ReadLSN(5)
	assert.Nil(t, err)
	assert.True(t, strings.HasPrefix(r, HolePrefix), "Read error: expect '%v*', get '%v'", HolePrefix, r)

	r, err = p.ReadLSN(12)
	assert.Nil(t, err)
	assert.True(t, strings.HasPrefix(r, HolePrefix), "Read error: expect '%v*', get '%v'", HolePrefix, r)

	assert.Nil(t, p.Assign(0, 16, 100))

	r, err = p.ReadGSN(105)
	assert.Nil(t, err)
	assert.True(t, strings.HasPrefix(r, HolePrefix), "Read error: expect '%v*', get '%v'", HolePrefix, r)

	r, err = p.ReadGSN(112)
	assert.Nil(t, err)
	assert.True(t, strings.HasPrefix(r, HolePrefix), "Read error: expect '%v*', get '%v'", HolePrefix, r)

	assert.Nil(t, p.Close())
	assert.Nil(t, os.RemoveAll("tmp"))
}

func TestWriteAcrossMultiSeg(t *testing.T) {
	p, err := NewPartition("tmp", 10)
	assert.Nil(t, err)
	assert.NotNil(t, p)

	hole := HolePrefix

	l, err := p.Write(hole, 8)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), l)

	l, err = p.Write(hole, 20)
	assert.Nil(t, err)
	assert.Equal(t, int64(8), l)

	r, err := p.ReadLSN(5)
	assert.Nil(t, err)
	assert.True(t, strings.HasPrefix(r, HolePrefix), "Read error: expect '%v*', get '%v'", HolePrefix, r)

	r, err = p.ReadLSN(12)
	assert.Nil(t, err)
	assert.True(t, strings.HasPrefix(r, HolePrefix), "Read error: expect '%v*', get '%v'", HolePrefix, r)

	r, err = p.ReadLSN(22)
	assert.Nil(t, err)
	assert.True(t, strings.HasPrefix(r, HolePrefix), "Read error: expect '%v*', get '%v'", HolePrefix, r)

	assert.Nil(t, p.Assign(0, 28, 100))

	r, err = p.ReadGSN(105)
	assert.Nil(t, err)
	assert.True(t, strings.HasPrefix(r, HolePrefix), "Read error: expect '%v*', get '%v'", HolePrefix, r)

	r, err = p.ReadGSN(112)
	assert.Nil(t, err)
	assert.True(t, strings.HasPrefix(r, HolePrefix), "Read error: expect '%v*', get '%v'", HolePrefix, r)

	r, err = p.ReadGSN(122)
	assert.Nil(t, err)
	assert.True(t, strings.HasPrefix(r, HolePrefix), "Read error: expect '%v*', get '%v'", HolePrefix, r)

	assert.Nil(t, p.Close())
	assert.Nil(t, os.RemoveAll("tmp"))
}

func TestWritePartitionMixed(t *testing.T) {
	p, err := NewPartition("tmp", 10)
	assert.Nil(t, err)
	assert.NotNil(t, p)

	record := "test"
	hole := HolePrefix

	l, err := p.Write(record, 0)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), l)

	l, err = p.Write(hole, 9)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), l)

	l, err = p.Write(record, 0)
	assert.Nil(t, err)
	assert.Equal(t, int64(10), l)

	r, err := p.ReadLSN(0)
	assert.Nil(t, err)
	assert.Equal(t, record, r)

	r, err = p.ReadLSN(9)
	assert.Nil(t, err)
	assert.True(t, strings.HasPrefix(r, HolePrefix), "Read error: expect '%v*', get '%v'", HolePrefix, r)

	r, err = p.ReadLSN(10)
	assert.Nil(t, err)
	assert.Equal(t, record, r)

	assert.Nil(t, p.Assign(0, 11, 100))

	r, err = p.ReadGSN(100)
	assert.Nil(t, err)
	assert.Equal(t, record, r)

	r, err = p.ReadGSN(109)
	assert.Nil(t, err)
	assert.True(t, strings.HasPrefix(r, HolePrefix), "Read error: expect '%v*', get '%v'", HolePrefix, r)

	r, err = p.ReadGSN(110)
	assert.Nil(t, err)
	assert.Equal(t, record, r)

	assert.Nil(t, p.Close())
	assert.Nil(t, os.RemoveAll("tmp"))
}
