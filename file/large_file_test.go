//go:build !race

package file_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"testing"

	"github.com/ipfs/go-cid"
	ipfsutil "github.com/ipfs/go-ipfs-util"
	"github.com/ipfs/go-unixfsnode/data/builder"
	"github.com/ipfs/go-unixfsnode/file"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

func TestLargeFileReader(t *testing.T) {
	if testing.Short() || strconv.IntSize == 32 {
		t.Skip()
	}
	buf := make([]byte, 512*1024*1024)
	ipfsutil.NewSeededRand(0xdeadbeef).Read(buf)
	r := bytes.NewReader(buf)

	ls := cidlink.DefaultLinkSystem()
	storage := cidlink.Memory{}
	ls.StorageReadOpener = storage.OpenRead
	ls.StorageWriteOpener = storage.OpenWrite

	f, _, err := builder.BuildUnixFSFile(r, "", &ls)
	if err != nil {
		t.Fatal(err)
	}

	// get back the root node substrate from the link at the top of the builder.
	fr, err := ls.Load(ipld.LinkContext{}, f, dagpb.Type.PBNode)
	if err != nil {
		t.Fatal(err)
	}

	ufn, err := file.NewUnixFSFile(context.Background(), fr, &ls)
	if err != nil {
		t.Fatal(err)
	}
	// read back out the file.
	for i := 0; i < len(buf); i += 100 * 1024 * 1024 {
		rs, err := ufn.AsLargeBytes()
		if err != nil {
			t.Fatal(err)
		}
		_, err = rs.Seek(int64(i), io.SeekStart)
		if err != nil {
			t.Fatal(err)
		}
		ob, err := io.ReadAll(rs)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(ob, buf[i:]) {
			t.Fatal("Not equal at offset", i, "expected", len(buf[i:]), "got", len(ob))
		}
	}
}

func TestLargeFileSeeker(t *testing.T) {
	ls := cidlink.DefaultLinkSystem()
	storage := cidlink.Memory{}
	ls.StorageReadOpener = storage.OpenRead
	ls.StorageWriteOpener = storage.OpenWrite

	// Make random file with 1024 bytes.
	buf := make([]byte, 1024)
	ipfsutil.NewSeededRand(0xdeadbeef).Read(buf)
	r := bytes.NewReader(buf)

	// Build UnixFS File chunked in 256 byte parts.
	f, _, err := builder.BuildUnixFSFile(r, "size-256", &ls)
	if err != nil {
		t.Fatal(err)
	}

	// Load the file.
	fr, err := ls.Load(ipld.LinkContext{}, f, dagpb.Type.PBNode)
	if err != nil {
		t.Fatal(err)
	}

	// Create it.
	ufn, err := file.NewUnixFSFile(context.Background(), fr, &ls)
	if err != nil {
		t.Fatal(err)
	}

	rs, err := ufn.AsLargeBytes()
	if err != nil {
		t.Fatal(err)
	}

	testSeekIn1024ByteFile(t, rs)
}

func TestLargeFileReaderReadsOnlyNecessaryBlocks(t *testing.T) {
	tracker, ls := mockTrackingLinkSystem()

	// Make random file with 1024 bytes.
	buf := make([]byte, 1024)
	ipfsutil.NewSeededRand(0xdeadbeef).Read(buf)
	r := bytes.NewReader(buf)

	// Build UnixFS File chunked in 256 byte parts.
	f, _, err := builder.BuildUnixFSFile(r, "size-256", ls)
	if err != nil {
		t.Fatal(err)
	}

	// Load the file.
	fr, err := ls.Load(ipld.LinkContext{}, f, dagpb.Type.PBNode)
	if err != nil {
		t.Fatal(err)
	}

	// Create it.
	ufn, err := file.NewUnixFSFile(context.Background(), fr, ls)
	if err != nil {
		t.Fatal(err)
	}

	// Prepare tracker for read.
	tracker.resetTracker()

	rs, err := ufn.AsLargeBytes()
	if err != nil {
		t.Fatal(err)
	}

	// Move the pointer to the 2nd block of the file.
	_, err = rs.Seek(256, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	// Read the 3rd and 4th blocks of the file.
	portion := make([]byte, 512)
	_, err = io.ReadAtLeast(rs, portion, 512)
	if err != nil {
		t.Fatal(err)
	}

	// Just be sure we read the right bytes.
	if !bytes.Equal(portion, buf[256:768]) {
		t.Fatal(fmt.Errorf("did not read correct bytes"))
	}

	// We must have read 2 CIDs for each of the 2 blocks!
	if l := len(tracker.cids); l != 2 {
		t.Fatal(fmt.Errorf("expected to have read 2 blocks, read %d", l))
	}
}

type trackingReadOpener struct {
	cidlink.Memory
	mu   sync.Mutex
	cids []cid.Cid
}

func (ro *trackingReadOpener) resetTracker() {
	ro.mu.Lock()
	ro.cids = nil
	ro.mu.Unlock()
}

func (ro *trackingReadOpener) OpenRead(lnkCtx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
	cidLink, ok := lnk.(cidlink.Link)
	if !ok {
		return nil, fmt.Errorf("invalid link type for loading: %v", lnk)
	}

	ro.mu.Lock()
	ro.cids = append(ro.cids, cidLink.Cid)
	ro.mu.Unlock()

	return ro.Memory.OpenRead(lnkCtx, lnk)
}

func mockTrackingLinkSystem() (*trackingReadOpener, *ipld.LinkSystem) {
	ls := cidlink.DefaultLinkSystem()
	storage := &trackingReadOpener{Memory: cidlink.Memory{}}

	ls.StorageWriteOpener = storage.OpenWrite
	ls.StorageReadOpener = storage.OpenRead
	ls.TrustedStorage = true

	return storage, &ls
}
