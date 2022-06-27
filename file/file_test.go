package file_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/ipfs/go-unixfsnode"
	"github.com/ipfs/go-unixfsnode/directory"
	"github.com/ipfs/go-unixfsnode/file"
	"github.com/ipld/go-car/v2/blockstore"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
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

func TestBasicnodeReify(t *testing.T) {
	baseFile := "./fixtures/QmT78zSuBmuS4z925WZfrqQ1qHaJ56DQaTfyMUF7F8ff5o.car"
	root, ls := open(baseFile, t)
	nb := basicnode.Prototype.Any.NewBuilder()
	err := datamodel.Copy(root, nb)
	if err != nil {
		t.Fatal(err)
	}
	basicroot := nb.Build()
	file, err := unixfsnode.Reify(ipld.LinkContext{}, basicroot, ls)
	if err != nil {
		t.Fatal(err)
	}
	if file == basicroot {
		t.Fatal("node pass-through with Reify()")
	}
	fc, err := file.AsBytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fc, []byte("hello world\n")) {
		t.Errorf("file content does not match: %s", string(fc))
	}
}

func TestReifyDagPB(t *testing.T) {
	baseFile := "./fixtures/QmT78zSuBmuS4z925WZfrqQ1qHaJ56DQaTfyMUF7F8ff5o.car"
	root, ls := open(baseFile, t)
	file, err := unixfsnode.Reify(ipld.LinkContext{}, root, ls)
	if err != nil {
		t.Fatal(err)
	}
	if file == root {
		t.Fatal("node pass-through with Reify()")
	}
	fc, err := file.AsBytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fc, []byte("hello world\n")) {
		t.Errorf("file content does not match: %s", string(fc))
	}
}

func TestBasicnodeReifyDagPBFails(t *testing.T) {
	baseFile := "./fixtures/QmT78zSuBmuS4z925WZfrqQ1qHaJ56DQaTfyMUF7F8ff5o.car"
	root, ls := open(baseFile, t)
	nb := basicnode.Prototype.Any.NewBuilder()
	err := datamodel.Copy(root, nb)
	if err != nil {
		t.Fatal(err)
	}
	basicroot := nb.Build()
	// we expect this to pass through without being interpreted as UnixFS since
	// ReifyDagPB is strict
	file, err := unixfsnode.ReifyDagPB(ipld.LinkContext{}, basicroot, ls)
	if err != nil {
		t.Fatal(err)
	}
	if file != basicroot {
		t.Fatal("node did not pass-through Reify")
	}
	_, err = file.AsBytes()
	if err == nil {
		t.Fatal("should not be able to AsBytes() unreified node")
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
