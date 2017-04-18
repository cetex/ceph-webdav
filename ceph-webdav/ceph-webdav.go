package main

import (
	"fmt"
	"github.com/ceph/go-ceph/rados"
	"golang.org/x/net/webdav"
	"log"
	"net/http"
	"os"
)

type Ceph struct {
	conn  *rados.Conn
	ioctx *rados.IOContext
	mdctx *rados.IOContext
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
		log.Println("FAIL!")
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
	log.Println("Creating IO context for pool: ", pool+"_metadata")
	if ioctx, err := c.conn.OpenIOContext(pool + "_metadata"); err != nil {
		log.Println(err)
		return err
	} else {
		c.mdctx = ioctx
	}
	log.Println("Initialized")
	return nil
}

func (c *Ceph) Mkdir(name string, perm os.FileMode) error {
	log.Printf("CephDAV: Mkdir: %v", name)
	return nil
}

func (c *Ceph) createFD(name string) *cephFile {
	return &cephFile{
		oid:  name,
		pos:  0,
		ceph: c,
	}
}

func (c *Ceph) OpenFile(name string, flag int, perm os.FileMode) (webdav.File, error) {
	log.Printf("CephDAV: OpenFile: %v", name)
	return c.createFD(name), nil
}

func (c *Ceph) RemoveAll(name string) error {
	log.Printf("CephDAV: Removeall: %v", name)
	return c.ioctx.Delete(name)
}

func (c *Ceph) Rename(oldName, newName string) error {
	log.Printf("CephDAV: Rename: %v -> %v", oldName, newName)
	oldf := c.createFD(oldName)
	defer oldf.Close()
	oldfStat, err := oldf.Stat()
	if err != nil {
		return err
	}
	newf := c.createFD(newName)
	defer newf.Close()
	buf := make([]byte, oldfStat.Size()) // create buf of filesize, this sucks but is a quick and dirty fix.
	read, err := oldf.Read(buf)
	if err != nil {
		return err
	}
	if int64(read) < oldfStat.Size() {
		return fmt.Errorf("Failed to read entire file")
	}
	write, err := newf.Write(buf)
	if err != nil {
		return err
	}
	if int64(write) != oldfStat.Size() {
		return fmt.Errorf("Failed to write entire new file")
	}
	return c.RemoveAll(oldName)

	// Ceph doesn't support renaming it seems..
	// We could read the file, write it and then delete the original
	// but that means we can set us up for quite long-running jobs..
	//return fmt.Errorf("Renaming not supported")
}

func (c *Ceph) Stat(name string) (os.FileInfo, error) {
	log.Printf("CephDAV: Stat: %v", name)
	f := c.createFD(name)
	return f.Stat()
}

func main() {
	c := new(Ceph)
	if err := c.connect("test"); err != nil {
		log.Println(err)
	}
	srv := &webdav.Handler{
		Prefix:     "/",
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
