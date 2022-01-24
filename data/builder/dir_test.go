package builder

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/stretchr/testify/require"
)

func mkEntries(cnt int, ls *ipld.LinkSystem) ([]dagpb.PBLink, error) {
	entries := make([]dagpb.PBLink, 0, cnt)
	for i := 0; i < cnt; i++ {
		r := bytes.NewBufferString(fmt.Sprintf("%d", i))
		f, s, err := BuildUnixFSFile(r, "", ls)
		if err != nil {
			return nil, err
		}
		e, err := BuildUnixFSDirectoryEntry(fmt.Sprintf("file %d", i), int64(s), f)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
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

	for i := 0; i < 1344; i++ {
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

	for i := 0; i < 1343; i++ {
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
