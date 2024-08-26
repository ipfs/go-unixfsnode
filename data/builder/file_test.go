package builder

import (
	"bytes"
	"context"
	"strconv"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
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
		bareExpected:    cid.MustParse("bafkreigwqvgm5f6vgdv7wjkttdhgnkpbazhvuzvrqzaje4scb4moeinjum"),
		wrappedExpected: cid.MustParse("bafybeib7rloaw4vl56brrnsetobsopu23e5ezoqxq4zorxxtljoeafcpca"),
	},
	{
		size:            10 * 1024,
		bareExpected:    cid.MustParse("bafkreihaxm6boumj2cwzbs3t3mnktfsgcf25ratcvtcf5kqnsymgk2gxqy"),
		wrappedExpected: cid.MustParse("bafybeieogamws33kfbtpk5mdhoo2wkxwmd7dwnduyvo7wo65ll75d36xgi"),
	},
	{
		size:            100 * 1024,
		bareExpected:    cid.MustParse("bafkreia7ockt35s5ki5qzrm37bp57woott6bju6gw64wl7rus7xwjcoemq"),
		wrappedExpected: cid.MustParse("bafybeicywdnaqrwj3t7xltqgtaoi3ebk6fi2oyam6gsqle3bl4piucpzua"),
	},
	{
		size: 10 * 1024 * 1024,
		// https://github.com/ipfs/go-unixfs/blob/a7243ebfc36eaa89d79a39d3cef3fa1e60f7e49e/importer/importer_test.go#L49C1-L49C1
		// QmZN1qquw84zhV4j6vT56tCcmFxaDaySL1ezTXFvMdNmrK, but with --cid-version=1 all the way through the DAG
		bareExpected:    cid.MustParse("bafybeibxlkafr6oqgflgjcjfbl5db6agozxdknpludvh7ym54oa5qoowbm"),
		wrappedExpected: cid.MustParse("bafybeigqbp6jog6fvxbpq4opzcgn5rsp7xqrk7xa4zbgnqo6htjmolt3iy"),
	},
}

func TestBuildUnixFSFile_Reference(t *testing.T) {
	for _, tc := range referenceTestCases {
		t.Run(strconv.Itoa(tc.size), func(t *testing.T) {
			buf := make([]byte, tc.size)
			random.NewSeededRand(0xdeadbeef).Read(buf)
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
	random.NewSeededRand(0xdeadbeef).Read(buf)
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
