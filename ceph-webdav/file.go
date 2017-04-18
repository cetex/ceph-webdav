package main

import (
	"fmt"
	"github.com/ceph/go-ceph/rados"
	"github.com/davecgh/go-spew/spew"
	"log"
	"os"
	"time"
)

type cephFile struct {
	oid  string
	pos  uint64
	ceph *Ceph
}

func (f *cephFile) Close() error {
	log.Println("Close: ", f.oid)
	f.oid = ""
	f.pos = 0
	f.ceph = nil
	return nil
}

func (f *cephFile) Read(p []byte) (int, error) {
	log.Println("Read: ", f.oid)
	read, err := f.ceph.ioctx.Read(f.oid, p, f.pos)
	f.pos += uint64(read)
	return read, err
}

func (f *cephFile) Seek(offset int64, whence int) (int64, error) {
	log.Println("Seek: ", f.oid)
	switch whence {
	case 0: // Seek from start of file
		f.pos = uint64(offset)
	case 1: // Seek from current position
		f.pos += uint64(offset)
	case 2: // Seek from end of file
		stat, err := f.Stat()
		if err != nil {
			return int64(f.pos), fmt.Errorf("Failed to get current object size")
		}
		f.pos = uint64(stat.Size() + offset)
	}
	return int64(f.pos), nil
}

func (f *cephFile) Write(p []byte) (int, error) {
	log.Println("Write: ", f.oid)
	err := f.ceph.ioctx.Write(f.oid, p, f.pos)
	if err != nil {
		// If error, assume nothing was written. Ceph should be fully
		// consistent and if write fails without info on how much was
		// written, we have to assume it was aborted.
		return 0, err
	}
	f.pos += uint64(len(p))
	return len(p), nil
}

func (f *cephFile) Readdir(count int) ([]os.FileInfo, error) {
	log.Println("Readdir: ", f.oid)
	if f.oid == "" {
		// Is root directory, create file listing.
		dirList := []os.FileInfo{}
		if root, err := f.rootStat(); err != nil {
			return nil, err
		} else {
			dirList = append(dirList, root)
		}
		iter, err := f.ceph.ioctx.Iter()
		if err != nil {
			return nil, err
		}
		defer iter.Close()
		for iter.Next() {
			log.Printf("%v\n", iter.Value())
			stat, err := f.ceph.Stat(iter.Value())
			if err != nil {
				log.Println("Error in Readdir / stat: ", err)
			}
			dirList = append(dirList, stat)
		}
		return dirList, iter.Err()
	}
	return nil, fmt.Errorf("Not a directory!")
}

func (f *cephFile) rootStat() (*cephStat, error) {
	stat, err := f.ceph.ioctx.GetPoolStats()
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return &cephStat{
		name:    "",
		size:    int64(stat.Num_bytes),
		mode:    os.FileMode(755) + 1<<(32-1),
		modTime: time.Now(),
		isDir:   true,
		sys:     nil,
	}, nil
}

func (f *cephFile) Stat() (os.FileInfo, error) {
	log.Println("Stat: ", f.oid)
	if f.oid == "" {
		// Stat on root directory, make up a directory..
		return f.rootStat()
	}
	stat, err := f.ceph.ioctx.Stat(f.oid)
	if err != nil {
		log.Println(err)
		spew.Dump(err)
		switch err {
		case rados.RadosErrorNotFound:
			return nil, os.ErrNotExist
		default:
			return nil, err
		}
	}
	return &cephStat{
		name:    f.oid,
		size:    int64(stat.Size),
		mode:    os.FileMode(0644),
		modTime: stat.ModTime,
		isDir:   false,
		sys:     nil,
	}, nil
}

type cephStat struct {
	name     string
	size     int64
	mode     os.FileMode
	modTime  time.Time
	isDir    bool
	sys      interface{}
	statDone bool
}

func (s *cephStat) Name() string {
	log.Printf("Name: ", s.name)
	return s.name
}

func (s *cephStat) Size() int64 {
	log.Printf("Size: ", s.size)
	return s.size
}

func (s *cephStat) Mode() os.FileMode {
	log.Printf("Mode: ", s.mode)
	return s.mode
}

func (s *cephStat) ModTime() time.Time {
	log.Printf("ModTime", s.modTime)
	return s.modTime
}

func (s *cephStat) IsDir() bool {
	log.Printf("IsDir: ", s.isDir)
	return s.isDir
}

func (s *cephStat) Sys() interface{} {
	log.Printf("Sys: ")
	return nil
}
