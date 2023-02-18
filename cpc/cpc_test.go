// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

package cpc

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	mathrand "math/rand"
	"path/filepath"
	"runtime"
	"testing"
)

func TestCopyBlockwise(t *testing.T) {
	sizes := []int64{0, 1}
	for n := 1; n < runtime.NumCPU(); n++ {
		for delta := -1; delta <= 1; delta++ {
			sizes = append(sizes, int64(n*(4<<10)+delta))
		}
	}
	td := t.TempDir()
	for _, size := range sizes {
		t.Run(fmt.Sprintf("size%d", size), func(t *testing.T) {
			ss := fmt.Sprint(size)
			src := filepath.Join(td, "input-"+ss)
			dst := filepath.Join(td, "output-"+ss)
			want := randBytes(int(size))
			if err := ioutil.WriteFile(src, want, 0644); err != nil {
				t.Fatal(err)
			}
			// 3 runs: initial copy, no-op copy, single byte dirtied copy
			for run := 1; run <= 3; run++ {
				t.Run(fmt.Sprintf("run%d", run), func(t *testing.T) {
					// On the 3rd run, dirty one random byte of the dst file.
					if run == 3 {
						if size == 0 {
							t.Skip("n/a")
						}
						dirtyCopy := append([]byte(nil), want...)
						dirtyCopy[mathrand.Intn(int(size))] = byte(mathrand.Intn(256))
						if err := ioutil.WriteFile(dst, dirtyCopy, 0644); err != nil {
							t.Fatal(err)
						}
					}
					st, err := Copy(context.Background(), loggerDiscard, src, dst)
					if err != nil {
						t.Fatalf("cpblockwise: %v", err)
					}
					if run == 1 && st.PagesUnmodified != 0 {
						t.Errorf("initial unmodified pages = %v; want 0", st.PagesUnmodified)
					}
					if run == 2 && st.PagesWritten > 0 {
						t.Errorf("second run written pages = %v; want 0", st.PagesWritten)
					}
					if run == 3 {
						if st.PagesWritten != 1 {
							t.Errorf("PagesWritten = %v; want 1", st.PagesWritten)
						}
						if size > 4<<10 && st.PagesUnmodified == 0 {
							t.Errorf("PagesUnmodified = %v; want >0", st.PagesUnmodified)
						}
					}
					got, err := ioutil.ReadFile(dst)
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(got, want) {
						t.Fatalf("bytes didn't equal; dst len = %v; want len %v", len(got), size)
					}
				})
			}
		})
	}
}

// loggerDiscard is a Logf that throws away the logs given to it.
func loggerDiscard(string, ...interface{}) {}

func randBytes(n int) []byte {
	ret := make([]byte, n)
	rand.Read(ret)
	return ret
}
