package builder

import (
	"bytes"
	"context"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/ipfs/go-cid"
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
		bareExpected:    cid.MustParse("bafkreifb7ur7vrppfjmfzdai64b7ps2kbknn3qm2h5rgekisk4p2invbma"),
		wrappedExpected: cid.MustParse("bafybeid2kylejxunpqkja6n6mmkurkqresul35wyqebeemfzeqi3sdpuly"),
	},
	{
		size:            10 * 1024,
		bareExpected:    cid.MustParse("bafkreib4e6kdlkbqflqxuajrc3gaojmg5h76z2uwcgylgk77rhdiymxprm"),
		wrappedExpected: cid.MustParse("bafybeigcpq25l5lmit7yybb54k2m7azme32cux4ha42z2h6o7jhax2fpcm"),
	},
	{
		size:            100 * 1024,
		bareExpected:    cid.MustParse("bafkreibf7lmsphxirotzojspxp2cg4swevwade3uctwakw5ite7cfjcu4i"),
		wrappedExpected: cid.MustParse("bafybeig7pzfp2pamyj3umeusy5stwqulfsjucerrgwqgidur33dooomzze"),
	},
	{
		size: 10 * 1024 * 1024,
		// https://github.com/ipfs/go-unixfs/blob/a7243ebfc36eaa89d79a39d3cef3fa1e60f7e49e/importer/importer_test.go#L49C1-L49C1
		// QmZN1qquw84zhV4j6vT56tCcmFxaDaySL1ezTXFvMdNmrK, but with --cid-version=1 all the way through the DAG
		bareExpected:    cid.MustParse("bafybeihq2p6motnxdjdlhba7m6qt4xpor6n5rksasgfmfz4pq4jdh5p3e4"),
		wrappedExpected: cid.MustParse("bafybeiehtvgdwqebeqzelc7adwhbcvfohyj6k5fxfwfqbncmf22mk2ezxm"),
	},
}

func TestBuildUnixFSFile_Reference(t *testing.T) {
	rndSrc := rand.NewChaCha8(chacha8Seed)
	for _, tc := range referenceTestCases {
		t.Run(strconv.Itoa(tc.size), func(t *testing.T) {
			buf := make([]byte, tc.size)
			rndSrc.Read(buf)
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
	rand.NewChaCha8(chacha8Seed).Read(buf)
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
