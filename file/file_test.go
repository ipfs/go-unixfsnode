package file_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	ipfsutil "github.com/ipfs/go-ipfs-util"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipfs/go-unixfsnode/data/builder"
	"github.com/ipfs/go-unixfsnode/directory"
	"github.com/ipfs/go-unixfsnode/file"
	"github.com/ipld/go-car/v2/blockstore"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

func TestRootV0File(t *testing.T) {
	baseFile := "./fixtures/QmT78zSuBmuS4z925WZfrqQ1qHaJ56DQaTfyMUF7F8ff5o.car"
	root, ls := open(baseFile, t)
	file, err := file.NewUnixFSFile(context.Background(), root, ls)
	if err != nil {
		t.Fatal(err)
	}
	fc, err := file.AsBytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fc, []byte("hello world\n")) {
		t.Errorf("file content does not match: %s", string(fc))
	}
}

func TestNamedV0File(t *testing.T) {
	baseFile := "./fixtures/QmT8EC9sJq63SkDZ1mWLbWWyVV66PuqyHWpKkH4pcVyY4H.car"
	root, ls := open(baseFile, t)
	dir, err := unixfsnode.Reify(ipld.LinkContext{}, root, ls)
	if err != nil {
		t.Fatal(err)
	}
	dpbn := dir.(directory.UnixFSBasicDir)
	name, link := dpbn.Iterator().Next()
	if name.String() != "b.txt" {
		t.Fatal("unexpected filename")
	}
	fileNode, err := ls.Load(ipld.LinkContext{}, link.Link(), dagpb.Type.PBNode)
	if err != nil {
		t.Fatal(err)
	}
	file, err := file.NewUnixFSFile(context.Background(), fileNode, ls)
	if err != nil {
		t.Fatal(err)
	}
	fc, err := file.AsBytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fc, []byte("hello world\n")) {
		t.Errorf("file content does not match: %s", string(fc))
	}
}

func TestFileSeeker(t *testing.T) {
	ls := cidlink.DefaultLinkSystem()
	storage := cidlink.Memory{}
	ls.StorageReadOpener = storage.OpenRead
	ls.StorageWriteOpener = storage.OpenWrite

	// Make random file with 1024 bytes.
	buf := make([]byte, 1024)
	ipfsutil.NewSeededRand(0xdeadbeef).Read(buf)
	r := bytes.NewReader(buf)

	// Build UnixFS File as a single chunk
	f, _, err := builder.BuildUnixFSFile(r, "size-1024", &ls)
	if err != nil {
		t.Fatal(err)
	}

	// Load the file.
	fr, err := ls.Load(ipld.LinkContext{}, f, basicnode.Prototype.Bytes)
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

func open(car string, t *testing.T) (ipld.Node, *ipld.LinkSystem) {
	baseStore, err := blockstore.OpenReadOnly(car)
	if err != nil {
		t.Fatal(err)
	}
	ls := cidlink.DefaultLinkSystem()
	ls.StorageReadOpener = func(lctx ipld.LinkContext, l ipld.Link) (io.Reader, error) {
		cl, ok := l.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("couldn't load link")
		}
		blk, err := baseStore.Get(lctx.Ctx, cl.Cid)
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(blk.RawData()), nil
	}
	carRoots, err := baseStore.Roots()
	if err != nil {
		t.Fatal(err)
	}
	root, err := ls.Load(ipld.LinkContext{}, cidlink.Link{Cid: carRoots[0]}, dagpb.Type.PBNode)
	if err != nil {
		t.Fatal(err)
	}
	return root, &ls
}

func testSeekIn1024ByteFile(t *testing.T, rs io.ReadSeeker) {
	// Seek from the start and try reading
	offset, err := rs.Seek(128, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	if offset != 128 {
		t.Fatalf("expected offset %d, got %d", 484, offset)
	}

	readBuf := make([]byte, 256)
	_, err = io.ReadFull(rs, readBuf)
	if err != nil {
		t.Fatal(err)
	}

	// Validate we can detect the offset with SeekCurrent
	offset, err = rs.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}

	if offset != 384 {
		t.Fatalf("expected offset %d, got %d", 384, offset)
	}

	// Validate we can read after moving with SeekCurrent
	offset, err = rs.Seek(100, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 484 {
		t.Fatalf("expected offset %d, got %d", 484, offset)
	}

	_, err = io.ReadFull(rs, readBuf)
	if err != nil {
		t.Fatal(err)
	}

	offset, err = rs.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}

	if offset != 740 {
		t.Fatalf("expected offset %d, got %d", 740, offset)
	}

	// Validate we can read after moving with SeekEnd
	offset, err = rs.Seek(-400, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}

	if offset != 624 {
		t.Fatalf("expected offset %d, got %d", 624, offset)
	}

	_, err = io.ReadFull(rs, readBuf)
	if err != nil {
		t.Fatal(err)
	}

	offset, err = rs.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}

	if offset != 880 {
		t.Fatalf("expected offset %d, got %d", 880, offset)
	}
}
