package builder

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
	"github.com/ipfs/go-unixfsnode"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func mkEntries(cnt int, ls *ipld.LinkSystem) ([]dagpb.PBLink, error) {
	entries := make([]dagpb.PBLink, 0, cnt)
	for i := range cnt {
		r := bytes.NewBufferString(fmt.Sprintf("%d", i))
		e, err := mkEntry(r, fmt.Sprintf("file %d", i), ls)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

func mkEntry(r io.Reader, name string, ls *ipld.LinkSystem) (dagpb.PBLink, error) {
	f, s, err := BuildUnixFSFile(r, "", ls)
	if err != nil {
		return nil, err
	}
	return BuildUnixFSDirectoryEntry(name, int64(s), f)
}

func TestBuildUnixFSFileWrappedInDirectory_Reference(t *testing.T) {
	for _, tc := range referenceTestCases {
		t.Run(strconv.Itoa(tc.size), func(t *testing.T) {
			buf := make([]byte, tc.size)
			random.NewSeededRand(0xdeadbeef).Read(buf)
			r := bytes.NewReader(buf)

			ls := cidlink.DefaultLinkSystem()
			storage := cidlink.Memory{}
			ls.StorageReadOpener = storage.OpenRead
			ls.StorageWriteOpener = storage.OpenWrite

			e, err := mkEntry(r, fmt.Sprintf("%d", tc.size), &ls)
			require.NoError(t, err)
			d, sz, err := BuildUnixFSDirectory([]dagpb.PBLink{e}, &ls)
			require.NoError(t, err)
			require.Equal(t, tc.wrappedExpected.String(), d.(cidlink.Link).Cid.String())

			// check sz is the stored size of all blocks in the generated DAG
			var totStored int
			for _, blk := range storage.Bag {
				totStored += len(blk)
			}
			require.Equal(t, totStored, int(sz))
		})
	}
}

// Cross-impl reference test: directory of files with single character
// names, starting from ' ' and ending with '~', but excluding the special
// characters '/' and '.'. Each file should contain a single byte with the
// same value as the character in its name. Files are added to a sharded
// directory with a fanout of 16, using CIDv1 throughout, and should result
// in the root CID of:
//
//	bafybeihnipspiyy3dctpcx7lv655qpiuy52d7b2fzs52dtrjqwmvbiux44
func TestBuildUnixFSDirectoryShardAltFanout_Reference(t *testing.T) {
	ls := cidlink.DefaultLinkSystem()
	storage := cidlink.Memory{}
	ls.StorageReadOpener = storage.OpenRead
	ls.StorageWriteOpener = storage.OpenWrite
	entries := make([]dagpb.PBLink, 0)
	for ch := ' '; ch <= '~'; ch++ {
		if ch == '/' || ch == '.' {
			continue
		}
		s := string(ch)
		r := bytes.NewBuffer([]byte(s))
		e, err := mkEntry(r, s, &ls)
		require.NoError(t, err)
		entries = append(entries, e)
	}
	lnk, sz, err := BuildUnixFSShardedDirectory(16, multihash.MURMUR3X64_64, entries, &ls)
	require.NoError(t, err)
	var totStored int
	for _, blk := range storage.Bag {
		totStored += len(blk)
	}
	require.Equal(t, totStored, int(sz))
	require.Equal(t, "bafybeihnipspiyy3dctpcx7lv655qpiuy52d7b2fzs52dtrjqwmvbiux44", lnk.String())
}

func TestBuildUnixFSDirectory(t *testing.T) {
	ls := cidlink.DefaultLinkSystem()
	storage := cidlink.Memory{}
	ls.StorageReadOpener = storage.OpenRead
	ls.StorageWriteOpener = storage.OpenWrite

	testSizes := []int{100, 1000, 50000}
	for _, cnt := range testSizes {
		entries, err := mkEntries(cnt, &ls)
		if err != nil {
			t.Fatal(err)
		}

		dl, _, err := BuildUnixFSDirectory(entries, &ls)
		if err != nil {
			t.Fatal(err)
		}

		pbn, err := ls.Load(ipld.LinkContext{}, dl, dagpb.Type.PBNode)
		if err != nil {
			t.Fatal(err)
		}
		ufd, err := unixfsnode.Reify(ipld.LinkContext{}, pbn, &ls)
		if err != nil {
			t.Fatal(err)
		}
		observedCnt := 0

		li := ufd.MapIterator()
		for !li.Done() {
			_, _, err := li.Next()
			if err != nil {
				t.Fatal(err)
			}
			observedCnt++
		}
		if observedCnt != cnt {
			fmt.Printf("%+v\n", ufd)
			t.Fatalf("unexpected number of dir entries %d vs %d", observedCnt, cnt)
		}
	}
}

func TestBuildUnixFSRecursive(t *testing.T) {
	// only the top CID is of interest, but this tree is correct and can be used for future validation
	fixture := fentry{
		"rootDir",
		"",
		mustCidDecode("bafybeihswl3f7pa7fueyayewcvr3clkdz7oetv4jolyejgw26p6l3qzlbm"),
		[]fentry{
			{"a", "aaa", mustCidDecode("bafkreieygsdw3t5qlsywpjocjfj6xjmmjlejwgw7k7zi6l45bgxra7xi6a"), nil},
			{
				"b",
				"",
				mustCidDecode("bafybeibohj54uixf2mso4t53suyarv6cfuxt6b5cj6qjsqaa2ezfxnu5pu"),
				[]fentry{
					{"1", "111", mustCidDecode("bafkreihw4cq6flcbsrnjvj77rkfkudhlyevdxteydkjjvvopqefasdqrvy"), nil},
					{"2", "222", mustCidDecode("bafkreie3q4kremt4bhhjdxletm7znjr3oqeo6jt4rtcxcaiu4yuxgdfwd4"), nil},
				},
			},
			{"c", "ccc", mustCidDecode("bafkreide3ksevvet74uks3x7vnxhp4ltfi6zpwbsifmbwn6324fhusia7y"), nil},
		},
	}

	ls := cidlink.DefaultLinkSystem()
	storage := cidlink.Memory{}
	ls.StorageReadOpener = storage.OpenRead
	ls.StorageWriteOpener = storage.OpenWrite

	dir := t.TempDir()
	makeFixture(t, dir, fixture)

	lnk, sz, err := BuildUnixFSRecursive(filepath.Join(dir, fixture.name), &ls)
	require.NoError(t, err)
	require.Equal(t, fixture.expectedLnk.String(), lnk.String())
	require.Equal(t, uint64(245), sz)
}

func TestBuildUnixFSRecursiveLargeSharded(t *testing.T) {
	// only the top CID is of interest, but this tree is correct and can be used for future validation
	fixture := fentry{
		"rootDir",
		"",
		mustCidDecode("bafybeigyvxs6og5jbmpaa43qbhhd5swklqcfzqdrtjgfh53qjon6hpjaye"),
		make([]fentry, 0),
	}

	for i := range 1344 {
		name := fmt.Sprintf("long name to fill out bytes to make the sharded directory test flip over the sharded directory limit because link names are included in the directory entry %d", i)
		fixture.children = append(fixture.children, fentry{name, name, cid.Undef, nil})
	}

	ls := cidlink.DefaultLinkSystem()
	storage := cidlink.Memory{}
	ls.StorageReadOpener = storage.OpenRead
	ls.StorageWriteOpener = storage.OpenWrite

	dir := t.TempDir()
	makeFixture(t, dir, fixture)

	lnk, sz, err := BuildUnixFSRecursive(filepath.Join(dir, fixture.name), &ls)
	require.NoError(t, err)
	require.Equal(t, fixture.expectedLnk.String(), lnk.String())
	require.Equal(t, uint64(515735), sz)
}

// Same as TestBuildUnixFSRecursiveLargeSharded but it's one file less which flips
// it back to the un-sharded format. So we're testing the boundary condition and
// the proper construction of large DAGs.
func TestBuildUnixFSRecursiveLargeUnsharded(t *testing.T) {
	// only the top CID is of interest, but this tree is correct and can be used for future validation
	fixture := fentry{
		"rootDir",
		"",
		mustCidDecode("bafybeihecq4rpl4nw3cgfb2uiwltgsmw5sutouvuldv5fxn4gfbihvnalq"),
		make([]fentry, 0),
	}

	for i := range 1343 {
		name := fmt.Sprintf("long name to fill out bytes to make the sharded directory test flip over the sharded directory limit because link names are included in the directory entry %d", i)
		fixture.children = append(fixture.children, fentry{name, name, cid.Undef, nil})
	}

	ls := cidlink.DefaultLinkSystem()
	storage := cidlink.Memory{}
	ls.StorageReadOpener = storage.OpenRead
	ls.StorageWriteOpener = storage.OpenWrite

	dir := t.TempDir()
	makeFixture(t, dir, fixture)

	lnk, sz, err := BuildUnixFSRecursive(filepath.Join(dir, fixture.name), &ls)
	require.NoError(t, err)
	require.Equal(t, fixture.expectedLnk.String(), lnk.String())
	require.Equal(t, uint64(490665), sz)
}

type fentry struct {
	name        string
	content     string
	expectedLnk cid.Cid
	children    []fentry
}

func makeFixture(t *testing.T, dir string, fixture fentry) {
	path := filepath.Join(dir, fixture.name)
	if fixture.children != nil {
		require.NoError(t, os.Mkdir(path, 0755))
		for _, c := range fixture.children {
			makeFixture(t, path, c)
		}
	} else {
		os.WriteFile(path, []byte(fixture.content), 0644)
	}
}

func mustCidDecode(s string) cid.Cid {
	c, err := cid.Decode(s)
	if err != nil {
		panic(err)
	}
	return c
}
