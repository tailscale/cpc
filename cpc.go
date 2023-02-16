// The cpblockwise command is like cp but optimized for files like SQLite that
// are only written at 4K page granularity. It writes the dest file in-place
// and tries to not write a page that's identical.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

const pgSize = 4 << 10

type Page struct {
	Off int64 // always 4K aligned
	Len int   // usually 4K, except at the tail
}

func main() {
	flag.Parse()
	n := flag.NArg()
	if n < 2 {
		log.Fatalf("usage: cpblockwise <from...> <to>")
	}
	last := flag.Arg(n - 1)
	var lastIsDir bool
	if fi, err := os.Stat(last); err == nil && fi.IsDir() {
		lastIsDir = true
	}
	if n > 2 && !lastIsDir {
		log.Fatalf("with more than two arguments, final one must be a directory")
	}
	// Directory copy mode.
	if lastIsDir {
		for _, srcName := range flag.Args()[:n-1] {
			dstName := filepath.Join(last, filepath.Base(srcName))
			if _, err := cpBlockwise(log.Printf, srcName, dstName); err != nil {
				log.Fatal(err)
			}
		}
		return
	}
	// Single file copy mode.
	srcName, dstName := flag.Arg(0), flag.Arg(1)
	if _, err := cpBlockwise(log.Printf, srcName, dstName); err != nil {
		log.Fatal(err)
	}
}

type stats struct {
	PagesWritten    int64
	PagesUnmodified int64
}

type Logf func(format string, args ...interface{})

func cpBlockwise(logf Logf, srcName, dstName string) (*stats, error) {
	numCPU := runtime.NumCPU()

	t0 := time.Now()
	srcF, err := os.Open(srcName)
	if err != nil {
		return nil, err
	}
	fi, err := srcF.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("only copies regular files; src %v is %v", srcName, fi.Mode())
	}
	size := fi.Size()

	dstF, err := os.OpenFile(dstName, os.O_CREATE|os.O_RDWR, fi.Mode().Perm())
	if err != nil {
		return nil, err
	}
	if err := dstF.Truncate(size); err != nil {
		return nil, err
	}

	pages := 0
	workc := make(chan Page, size/pgSize+1)
	remainSize := size
	off := int64(0)
	for remainSize > 0 {
		chunkSize := remainSize
		if chunkSize > pgSize {
			chunkSize = pgSize
		}
		p := Page{Off: off, Len: int(chunkSize)}
		remainSize -= chunkSize
		off += chunkSize
		pages++
		workc <- p
	}
	close(workc)

	logf("file %v is %v bytes, %v pages", srcName, size, pages)
	logf("over %v CPUs, %v pages per CPU", numCPU, pages/numCPU)

	var pagesUnmodified atomicInt64
	var pagesWritten atomicInt64
	var pagesTotal atomicInt64

	copyPage := func(p Page, bufSrc, bufDst []byte) error {
		bufSrc = bufSrc[:p.Len]
		bufDst = bufDst[:p.Len]
		// Note: ReadAt doesn't do short reads like io.Reader. Also, these two
		// ReadAt calls could be in theory be concurrent but we're already
		// running NumCPUs goroutines, so it wouldn't really help.
		if _, err := srcF.ReadAt(bufSrc, p.Off); err != nil {
			return err
		}
		if _, err := dstF.ReadAt(bufDst, p.Off); err != nil {
			return err
		}
		if bytes.Equal(bufSrc, bufDst) {
			pagesUnmodified.Add(1)
			return nil
		}
		if _, err := dstF.WriteAt(bufSrc, p.Off); err != nil {
			return err
		}
		pagesWritten.Add(1)
		return nil
	}

	var lastPrint atomic.Value // of time.Time
	printProgress := func() {
		logf("%0.2f%% done; %v pages written, %v unchanged",
			float64(pagesTotal.Load())*100/float64(pages),
			pagesWritten.Load(),
			pagesUnmodified.Load())
	}

	grp, ctx := errgroup.WithContext(context.Background())
	for i := 0; i < numCPU; i++ {
		grp.Go(func() error {
			bufSrc := make([]byte, pgSize)
			bufDst := make([]byte, pgSize)
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case p, ok := <-workc:
					if !ok {
						return nil
					}
					if err := copyPage(p, bufSrc, bufDst); err != nil {
						return err
					}
					done := pagesTotal.Add(1)
					lastPrintTime, _ := lastPrint.Load().(time.Time)
					if done%100 == 0 && time.Since(lastPrintTime) > time.Second {
						printProgress()
						lastPrint.Store(time.Now())
					}
				}
			}
		})
	}
	if err := grp.Wait(); err != nil {
		return nil, err
	}
	printProgress()
	logf("Done in %v", time.Since(t0).Round(time.Millisecond))
	if pagesWritten.Load()+pagesUnmodified.Load() != int64(pages) {
		return nil, fmt.Errorf("not consistent; expected %v pages total", pages)
	}
	return &stats{
		PagesWritten:    pagesWritten.Load(),
		PagesUnmodified: pagesUnmodified.Load(),
	}, nil
}

// atomicInt64 is sync/atomic.Int64, but this package is targeting
// pretty ancient Go.
type atomicInt64 int64

func (x *atomicInt64) Load() int64       { return atomic.LoadInt64((*int64)(x)) }
func (x *atomicInt64) Add(v int64) int64 { return atomic.AddInt64((*int64)(x), v) }
