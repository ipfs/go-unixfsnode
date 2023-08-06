package builder_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"testing"

	"github.com/ipfs/go-unixfsnode/data/builder"
	"github.com/multiformats/go-multicodec"

	"github.com/ipfs/go-cid"
	u "github.com/ipfs/go-ipfs-util"
	"github.com/ipfs/go-unixfsnode/file"
	"github.com/ipld/go-car/v2"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
)

func TestBuildUnixFSFile(t *testing.T) {
	buf := make([]byte, 10*1024*1024)
	u.NewSeededRand(0xdeadbeef).Read(buf)
	r := bytes.NewReader(buf)

	ls := cidlink.DefaultLinkSystem()
	storage := cidlink.Memory{}
	ls.StorageReadOpener = storage.OpenRead
	ls.StorageWriteOpener = storage.OpenWrite

	f, _, err := builder.BuildUnixFSFile(r, "", &ls)
	if err != nil {
		t.Fatal(err)
	}

	// Note: this differs from the previous
	// go-unixfs version of this test (https://github.com/ipfs/go-unixfs/blob/master/importer/importer_test.go#L50)
	// because this library enforces CidV1 encoding.
	expected, err := cid.Decode("bafybeieyxejezqto5xwcxtvh5tskowwxrn3hmbk3hcgredji3g7abtnfkq")
	if err != nil {
		t.Fatal(err)
	}
	if !expected.Equals(f.(cidlink.Link).Cid) {
		t.Fatalf("expected CID %s, got CID %s", expected, f)
	}
	if _, err := storage.OpenRead(ipld.LinkContext{}, f); err != nil {
		t.Fatal("expected top of file to be in store.")
	}
}

func TestEstimateUnixFSFileDefaultChunking(t *testing.T) {
	for i := 100; i < 1000000000; i *= 10 {
		b := make([]byte, i)
		rand.Read(b)

		ls := cidlink.DefaultLinkSystem()
		storage := cidlink.Memory{}
		ls.StorageReadOpener = storage.OpenRead
		nPB := 0

		ls.StorageWriteOpener = func(lc linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
			w, bwc, err := storage.OpenWrite(lc)
			return w, func(lnk ipld.Link) error {
				if lnk.(cidlink.Link).Cid.Prefix().Codec == uint64(multicodec.DagPb) {
					nPB++
				}
				return bwc(lnk)
			}, err
		}
		rt, _, err := builder.BuildUnixFSFile(bytes.NewReader(b), "", &ls)
		if err != nil {
			t.Fatal(err)
		}

		ob := bytes.NewBuffer(nil)
		_, err = car.TraverseV1(context.Background(), &ls, rt.(cidlink.Link).Cid, selectorparse.CommonSelector_ExploreAllRecursively, ob)
		if err != nil {
			t.Fatal(err)
		}
		fileLen := len(ob.Bytes())

		estimate := builder.EstimateUnixFSFileDefaultChunking(uint64(i))
		if estimate != uint64(fileLen) {
			fmt.Printf("%d intermediate nodes.\n", nPB)
			t.Fatalf("estimate for file length %d was %d. should be %d", i, estimate, fileLen)
		}
	}
}

func TestUnixFSFileRoundtrip(t *testing.T) {
	buf := make([]byte, 10*1024*1024)
	u.NewSeededRand(0xdeadbeef).Read(buf)
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
	out, err := ufn.AsBytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, buf) {
		t.Fatal("Not equal")
	}
}
