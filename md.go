package main

import (
	"fmt"
	"github.com/ceph/go-ceph/rados"
	"github.com/howeyc/crc16"
	"log"
	"os"
	"strings"
	"time"
	"unsafe"
)

type md struct {
	ioctx *rados.IOContext
	cache map[string]map[string]cephStat
}

// Encode entry v into b as LittleEndian, starting from position p
func PutUint64(b []byte, v uint64, p int) {
	_ = b[7+p] // early bounds check to guarantee safety of writes below
	b[0+p] = byte(v)
	b[1+p] = byte(v >> 8)
	b[2+p] = byte(v >> 16)
	b[3+p] = byte(v >> 24)
	b[4+p] = byte(v >> 32)
	b[5+p] = byte(v >> 40)
	b[6+p] = byte(v >> 48)
	b[7+p] = byte(v >> 56)
}

// Encode entry v into b as LittleEndian, starting from position p
func PutUint32(b []byte, v uint32, p int) {
	_ = b[3+p] // early bounds check to guarantee safety of writes below
	b[0+p] = byte(v)
	b[1+p] = byte(v >> 8)
	b[2+p] = byte(v >> 16)
	b[3+p] = byte(v >> 24)
}

// Encode entry v into b as LittleEndian, starting from position p
func PutUint16(b []byte, v uint16, p int) {
	_ = b[1+p] // early bounds check to guarantee safety of writes below
	b[0+p] = byte(v)
	b[1+p] = byte(v >> 8)
}

func makeMdEntry(state byte, f cephStat) []byte {
	nLength := len(f.name)
	entry := make([]byte, 23+nLength)
	// write out state first.
	entry[0] = state
	// Encode f.size (int64) in the byte slice as uint64
	PutUint64(entry, uint64(f.size), 1)
	// Encode f.modTime.Unix() in the byte slice as uint64
	PutUint64(entry, uint64(f.modTime.Unix()), 9)
	PutUint32(entry, uint32(f.modTime.Nanosecond()), 17)
	// Encode len(f.name) in the byte slice as uint16
	PutUint16(entry, uint16(nLength), 21)
	// write out f.name into byte slice
	for p := 0; p <= nLength; p++ {
		entry[p+23] = f.name[p]
	}
	PutUint16(entry, crc16.ChecksumCCITT(entry[0:23+nLength]), 23+nLength)
	return entry
}

func parseMdEntry(entry []byte) (byte, *cephStat, error) {
	// Entry looks like:
	// add(+)/remove(-) uint64(size of file) int64(modTime seconds) int32(modTime nanoSeconds) length(of filename) []byte(<filename>) uint16(checksum)
	// [+-]int16<file>uint64int64int32
	state := entry[0]
	if len(entry) < 24 {
		return state, nil, fmt.Errorf("DecodeError: Metadata expected entry of min size: %v, got %v", 24, len(entry))
	}
	size := *(*uint64)(unsafe.Pointer(&entry[1]))
	sTime := *(*int64)(unsafe.Pointer(&entry[9]))
	nTime := *(*int32)(unsafe.Pointer(&entry[17]))
	nLength := *(*int16)(unsafe.Pointer(&entry[21]))
	if len(entry) != 24+int(nLength) {
		return state, nil, fmt.Errorf("DecodeError: Metadata expected entry of size: %v, got: %v", 25+nLength, len(entry))
	}
	name := string(entry[23 : 23+nLength])
	crc := *(*uint16)(unsafe.Pointer(&entry[22+nLength]))
	if crc16.ChecksumCCITT(entry[0:21+nLength]) != crc {
		return 0, nil, fmt.Errorf("DecodeError: Metadata CRC error")
	}

	f := cephStat{
		name:    name,
		size:    int64(size),
		mode:    os.FileMode(644),
		modTime: time.Unix(sTime, int64(nTime)),
		isDir:   name[len(name)-1] == '/',
		sys:     nil}
	return state, &f, nil
}

func readMD(name string) (*[]cephStat, error) {
	if name == "" {
		name = "root/"
	}
	//files := new([]cephStat)
	buf := make([]byte, 1048576) // should make this a loop and parse stuff as i go..
	//read, err := f.ceph.mdctx.Read(name, buf, 0)
	read := 0
	err := fmt.Errorf("asd")
	if err != nil {
		log.Println("readMd ctx read: ", err)
		return nil, err
	}
	for _, file := range strings.Split(string(buf[:read]), "\n") {
		log.Println(parseMdEntry([]byte(file)))
	}
	return nil, nil
}
