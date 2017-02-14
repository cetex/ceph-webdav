package main

import (
	"fmt"
	"github.com/ceph/go-ceph/rados"
	"golang.org/x/net/webdav"
	"log"
	"net/http"
	"os"
	"time"
)

type cephFile struct {
	oid   string
	pos   uint64
	ioctx *rados.IOContext
	ceph  *Ceph
}

func (f *cephFile) Close() error {
	log.Println("Close: ", f.oid)
	f.oid = ""
	f.pos = 0
	f.ioctx = nil
	return nil
}

func (f *cephFile) Read(p []byte) (int, error) {
	log.Println("Read: ", f.oid)
	read, err := f.ioctx.Read(f.oid, p, f.pos)
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
	err := f.ioctx.Write(f.oid, p, f.pos)
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
	if f.oid == "/" {
		// Is root directory, create file listing.
		dirList := []os.FileInfo{}
		iter, err := f.ioctx.Iter()
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

func (f *cephFile) Stat() (os.FileInfo, error) {
	log.Println("Stat: ", f.oid)
	if f.oid == "/" {
		// Stat on root directory, make up a directory..
		stat, err := f.ioctx.GetPoolStats()
		if err != nil {
			log.Println(err)
			return nil, err
		}
		return &cephStat{
			name:    "/",
			size:    int64(stat.Num_bytes),
			mode:    os.FileMode(755) + 1<<(32-1),
			modTime: time.Now(),
			isDir:   true,
			sys:     nil,
		}, nil
	}
	stat, err := f.ioctx.Stat(f.oid)
	if err != nil {
		log.Println(err)
		return nil, err
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
	log.Printf("IsDir: ", s.IsDir)
	return s.isDir
}

func (s *cephStat) Sys() interface{} {
	log.Printf("Sys: ")
	return nil
}

type Ceph struct {
	conn  *rados.Conn
	ioctx *rados.IOContext
}

func (c *Ceph) connect(pool string) error {
	log.Println("Creating connection")
	if conn, err := rados.NewConn(); err != nil {
		log.Println(err)
		return err
	} else {
		c.conn = conn
	}
	log.Println("Reading config file")
	if err := c.conn.ReadDefaultConfigFile(); err != nil {
		log.Println(err)
		return err
	}
	log.Println("Connecting to ceph")
	if err := c.conn.Connect(); err != nil {
		log.Println(err)
		return err
	}
	log.Println("Creating IO Context for pool: ", pool)
	if ioctx, err := c.conn.OpenIOContext(pool); err != nil {
		log.Println(err)
		return err
	} else {
		c.ioctx = ioctx
	}
	log.Println("Initialized")
	return nil
}

func (c *Ceph) Mkdir(name string, perm os.FileMode) error {
	log.Printf("CephDAV: Mkdir: %v", name)
	return nil
}
func (c *Ceph) OpenFile(name string, flag int, perm os.FileMode) (webdav.File, error) {
	log.Printf("CephDAV: OpenFile: %v", name)
	return &cephFile{
		oid:   name,
		pos:   0,
		ioctx: c.ioctx,
		ceph:  c,
	}, nil
}
func (c *Ceph) RemoveAll(name string) error {
	log.Printf("CephDAV: Removeall: %v", name)
	return c.ioctx.Delete(name)
}
func (c *Ceph) Rename(oldName, newName string) error {
	log.Printf("CephDAV: Rename: %v -> %v", oldName, newName)
	// Ceph doesn't support renaming it seems..
	// We could read the file, write it and then delete the original
	// but that means we can set us up for quite long-running jobs..
	return fmt.Errorf("Renaming not supported")
}
func (c *Ceph) Stat(name string) (os.FileInfo, error) {
	log.Printf("CephDAV: Stat: %v", name)
	f := cephFile{
		oid:   name,
		pos:   0,
		ioctx: c.ioctx,
		ceph:  c,
	}
	return f.Stat()
}

func main() {
	c := new(Ceph)
	if err := c.connect("test"); err != nil {
		log.Println(err)
	}
	srv := &webdav.Handler{
		FileSystem: c,
		LockSystem: webdav.NewMemLS(),
		Logger: func(r *http.Request, err error) {
			log.Printf("WEBDAV: %#s, ERROR: %v", r, err)
		},
	}
	http.Handle("/", srv)
	if err := http.ListenAndServe(":8000", nil); err != nil {
		log.Fatalf("Error with WebDAV server: %v", err)
	}
}
