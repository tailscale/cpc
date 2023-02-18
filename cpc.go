// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

// The cpc command is like cp but optimized for files like SQLite that
// are only written at 4K page granularity. It writes the dest file in-place
// and tries to not write a page that's identical.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/tailscale/cpc/cpc"
)

func main() {
	flag.Parse()
	n := flag.NArg()
	if n < 2 {
		log.Fatalf("usage: cpc <from...> <to>")
	}
	last := flag.Arg(n - 1)
	var lastIsDir bool
	if fi, err := os.Stat(last); err == nil && fi.IsDir() {
		lastIsDir = true
	}
	if n > 2 && !lastIsDir {
		log.Fatalf("with more than two arguments, final one must be a directory")
	}
	ctx := context.Background()
	// Directory copy mode.
	if lastIsDir {
		for _, srcName := range flag.Args()[:n-1] {
			dstName := filepath.Join(last, filepath.Base(srcName))
			if _, err := cpc.Copy(ctx, log.Printf, srcName, dstName); err != nil {
				log.Fatal(err)
			}
		}
		return
	}
	// Single file copy mode.
	srcName, dstName := flag.Arg(0), flag.Arg(1)
	if _, err := cpc.Copy(ctx, log.Printf, srcName, dstName); err != nil {
		log.Fatal(err)
	}
}
