package builder

import (
	"bytes"
	"context"
	"strconv"
	"testing"

	"github.com/ipfs/go-cid"
	u "github.com/ipfs/go-ipfs-util"
	"github.com/ipfs/go-unixfsnode/file"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/stretchr/testify/require"
)

// referenceTestCases using older IPFS libraries, both bare forms of files sharded across raw leaves
// with CIDv1 and the same but wrapped in a directory with the name of the number of bytes.
var referenceTestCases = []struct {
	size            int
	bareExpected    cid.Cid
	wrappedExpected cid.Cid
}{
	{
		size:            1024,
		bareExpected:    cid.MustParse("bafkreibhn6gptq26tcez7zjklms4242pmhpiuql62ua2wlihyxdxua2nsa"),
		wrappedExpected: cid.MustParse("bafybeig6rdrdonmqxao32uihcnnbief4qdrjg4aktfo5fmu4cdeqya3glm"),
	},
	{
		size:            10 * 1024,
		bareExpected:    cid.MustParse("bafkreicesdeiwmnqq6i44so2sebekotb5zz7ymxv7fnbnynzrtftomk5l4"),
		wrappedExpected: cid.MustParse("bafybeihzqusxng5zb3qjtmkjizvwrv3jer2nafvcwlzhzs2p7sh7mswnsi"),
	},
	{
		size:            100 * 1024,
		bareExpected:    cid.MustParse("bafkreie72qttha6godppjndnmbyssddzh2ty2uog7cxwu3d5pzgn7nl72m"),
		wrappedExpected: cid.MustParse("bafybeidxgheulpeflagdewrjl7oe6loqtpfxncpieu6flje5hqbmgl5q7u"),
	},
	{
		size: 10 * 1024 * 1024,
		// https://github.com/ipfs/go-unixfs/blob/a7243ebfc36eaa89d79a39d3cef3fa1e60f7e49e/importer/importer_test.go#L49C1-L49C1
		// QmZN1qquw84zhV4j6vT56tCcmFxaDaySL1ezTXFvMdNmrK, but with --cid-version=1 all the way through the DAG
		bareExpected:    cid.MustParse("bafybeieyxejezqto5xwcxtvh5tskowwxrn3hmbk3hcgredji3g7abtnfkq"),
		wrappedExpected: cid.MustParse("bafybeieyal5cus7e4bazoffk5f2ltvlowjyne3z3axupo7lvvyq7dmy37m"),
	},
}

func TestBuildUnixFSFile_Reference(t *testing.T) {
	for _, tc := range referenceTestCases {
		t.Run(strconv.Itoa(tc.size), func(t *testing.T) {
			buf := make([]byte, tc.size)
			u.NewSeededRand(0xdeadbeef).Read(buf)
			r := bytes.NewReader(buf)

			ls := cidlink.DefaultLinkSystem()
			storage := cidlink.Memory{}
			ls.StorageReadOpener = storage.OpenRead
			ls.StorageWriteOpener = storage.OpenWrite

			f, sz, err := BuildUnixFSFile(r, "", &ls)
			require.NoError(t, err)
			require.Equal(t, tc.bareExpected.String(), f.(cidlink.Link).Cid.String())

			// check sz is the stored size of all blocks in the generated DAG
			var totStored int
			for _, blk := range storage.Bag {
				totStored += len(blk)
			}
			require.Equal(t, totStored, int(sz))
		})
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

	f, _, err := BuildUnixFSFile(r, "", &ls)
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
