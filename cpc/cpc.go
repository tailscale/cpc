// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

// Package cpc provides a copy function optimized for files like SQLite that
// are only written at 4K page granularity. It writes the dest file in-place
// and tries to not write a page that's identical.
package cpc

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

// Stats contains the stats of a copy operation.
type Stats struct {
	Duration        time.Duration
	PageSize        int64
	PagesWritten    int64
	PagesUnmodified int64
}

const pgSize = 4 << 10

// Page is a 4K page of a file.
type Page struct {
	Off int64 // always 4K aligned
	Len int   // usually 4K, except at the tail
}

// Logf is a logger that takes a format string and arguments.
type Logf func(format string, args ...interface{})

// Copy provides a concurrent blockwise copy of srcName to dstName.
func Copy(ctx context.Context, logf Logf, srcName, dstName string) (*Stats, error) {
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

	grp, ctx := errgroup.WithContext(ctx)
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
	d := time.Since(t0)
	logf("Done in %v", d.Round(time.Millisecond))
	if pagesWritten.Load()+pagesUnmodified.Load() != int64(pages) {
		return nil, fmt.Errorf("not consistent; expected %v pages total", pages)
	}
	return &Stats{
		Duration:        d,
		PageSize:        pgSize,
		PagesWritten:    pagesWritten.Load(),
		PagesUnmodified: pagesUnmodified.Load(),
	}, nil
}

// atomicInt64 is sync/atomic.Int64, but this package is targeting
// pretty ancient Go.
type atomicInt64 int64

func (x *atomicInt64) Load() int64       { return atomic.LoadInt64((*int64)(x)) }
func (x *atomicInt64) Add(v int64) int64 { return atomic.AddInt64((*int64)(x), v) }
