package hamt_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

	dag "github.com/ipfs/boxo/ipld/merkledag"
	mdtest "github.com/ipfs/boxo/ipld/merkledag/test"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	legacy "github.com/ipfs/boxo/ipld/unixfs/hamt"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-unixfsnode/hamt"
	"github.com/ipld/go-car/v2/storage"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/stretchr/testify/require"
)

// For now these tests use legacy UnixFS HAMT builders until we finish a builder
// in go-ipld-prime
func shuffle(seed int64, arr []string) {
	r := rand.New(rand.NewSource(seed))
	for i := 0; i < len(arr); i++ {
		a := r.Intn(len(arr))
		b := r.Intn(len(arr))
		arr[a], arr[b] = arr[b], arr[a]
	}
}

func makeDir(ds format.DAGService, size int) ([]string, *legacy.Shard, error) {
	return makeDirWidth(ds, size, 256)
}

func makeDirWidth(ds format.DAGService, size, width int) ([]string, *legacy.Shard, error) {
	ctx := context.Background()

	s, err := legacy.NewShard(ds, width)
	if err != nil {
		return nil, nil, err
	}

	var dirs []string
	for i := 0; i < size; i++ {
		dirs = append(dirs, fmt.Sprintf("DIRNAME%d", i))
	}

	shuffle(time.Now().UnixNano(), dirs)

	for i := 0; i < len(dirs); i++ {
		nd := ft.EmptyDirNode()
		err := ds.Add(ctx, nd)
		if err != nil {
			return nil, nil, err
		}
		err = s.Set(ctx, dirs[i], nd)
		if err != nil {
			return nil, nil, err
		}
	}

	return dirs, s, nil
}

func assertLinksEqual(linksA []*format.Link, linksB []*format.Link) error {

	if len(linksA) != len(linksB) {
		return fmt.Errorf("links arrays are different sizes")
	}

	sort.Stable(dag.LinkSlice(linksA))
	sort.Stable(dag.LinkSlice(linksB))
	for i, a := range linksA {
		b := linksB[i]
		if a.Name != b.Name {
			return fmt.Errorf("links names mismatch")
		}

		if a.Cid.String() != b.Cid.String() {
			return fmt.Errorf("link hashes dont match")
		}
	}

	return nil
}

func mockDag() (format.DAGService, *ipld.LinkSystem) {
	bsrv := mdtest.Bserv()
	dsrv := dag.NewDAGService(bsrv)
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lnkCtx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		cidLink, ok := lnk.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("invalid link type for loading: %v", lnk)
		}

		blk, err := bsrv.GetBlock(lnkCtx.Ctx, cidLink.Cid)
		if err != nil {
			return nil, err
		}

		return bytes.NewReader(blk.RawData()), nil
	}
	lsys.TrustedStorage = true
	return dsrv, &lsys
}

func TestBasicSet(t *testing.T) {
	ds, lsys := mockDag()
	for _, w := range []int{128, 256, 512, 1024} {
		t.Run(fmt.Sprintf("BasicSet%d", w), func(t *testing.T) {
			names, s, err := makeDirWidth(ds, 1000, w)
			require.NoError(t, err)
			ctx := context.Background()
			legacyNode, err := s.Node()
			require.NoError(t, err)
			nd, err := lsys.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: legacyNode.Cid()}, dagpb.Type.PBNode)
			require.NoError(t, err)
			hamtShard, err := hamt.AttemptHAMTShardFromNode(ctx, nd, lsys)
			require.NoError(t, err)
			for _, d := range names {
				_, err := hamtShard.LookupByString(d)
				require.NoError(t, err)
			}
		})
	}
}

func TestIterator(t *testing.T) {
	ds, lsys := mockDag()
	_, s, err := makeDir(ds, 300)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	legacyNode, err := s.Node()
	require.NoError(t, err)
	nds, err := legacy.NewHamtFromDag(ds, legacyNode)
	require.NoError(t, err)
	nd, err := lsys.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: legacyNode.Cid()}, dagpb.Type.PBNode)
	require.NoError(t, err)
	hamtShard, err := hamt.AttemptHAMTShardFromNode(ctx, nd, lsys)
	require.NoError(t, err)

	linksA, err := nds.EnumLinks(ctx)
	require.NoError(t, err)

	require.Equal(t, int64(len(linksA)), hamtShard.Length())

	linksB := make([]*format.Link, 0, len(linksA))
	iter := hamtShard.Iterator()
	for !iter.Done() {
		name, link := iter.Next()
		linksB = append(linksB, &format.Link{
			Name: name.String(),
			Cid:  link.Link().(cidlink.Link).Cid,
		})
	}
	require.NoError(t, assertLinksEqual(linksA, linksB))
}

func TestLoadFailsFromNonShard(t *testing.T) {
	ds, lsys := mockDag()
	ctx := context.Background()
	legacyNode := ft.EmptyDirNode()
	ds.Add(ctx, legacyNode)
	nd, err := lsys.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: legacyNode.Cid()}, dagpb.Type.PBNode)
	require.NoError(t, err)
	_, err = hamt.AttemptHAMTShardFromNode(ctx, nd, lsys)
	require.Error(t, err)

	// empty protobuf w/o data
	nd, err = qp.BuildMap(dagpb.Type.PBNode, -1, func(ma ipld.MapAssembler) {
		qp.MapEntry(ma, "Links", qp.List(-1, func(ipld.ListAssembler) {}))
	})
	require.NoError(t, err)

	_, err = hamt.AttemptHAMTShardFromNode(ctx, nd, lsys)
	require.Error(t, err)
}

func TestFindNonExisting(t *testing.T) {
	ds, lsys := mockDag()
	_, s, err := makeDir(ds, 100)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	legacyNode, err := s.Node()
	require.NoError(t, err)
	nd, err := lsys.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: legacyNode.Cid()}, dagpb.Type.PBNode)
	require.NoError(t, err)
	hamtShard, err := hamt.AttemptHAMTShardFromNode(ctx, nd, lsys)
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("notfound%d", i)
		_, err := hamtShard.LookupByString(key)
		require.EqualError(t, err, schema.ErrNoSuchField{Field: ipld.PathSegmentOfString(key)}.Error())
	}
}

func TestIncompleteShardedIteration(t *testing.T) {
	ctx := context.Background()
	req := require.New(t)

	fixture := "./fixtures/wikipedia-cryptographic-hash-function.car"
	f, err := os.Open(fixture)
	req.NoError(err)
	defer f.Close()
	carstore, err := storage.OpenReadable(f)
	req.NoError(err)
	lsys := cidlink.DefaultLinkSystem()
	lsys.TrustedStorage = true
	lsys.SetReadStorage(carstore)

	// classic recursive go-ipld-prime map iteration, being forgiving about
	// NotFound block loads to see what we end up with

	kvs := make(map[string]string)
	var iterNotFound int
	blockNotFound := make(map[string]struct{})

	var iter func(string, ipld.Link)
	iter = func(dir string, lnk ipld.Link) {
		nd, err := lsys.Load(ipld.LinkContext{Ctx: ctx}, lnk, basicnode.Prototype.Any)
		if nf, ok := err.(interface{ NotFound() bool }); ok && nf.NotFound() {
			// got a named link that we can't load
			blockNotFound[dir] = struct{}{}
			return
		}
		req.NoError(err)
		if nd.Kind() == ipld.Kind_Bytes {
			bv, err := nd.AsBytes()
			req.NoError(err)
			kvs[dir] = string(bv)
			return
		}

		nb := dagpb.Type.PBNode.NewBuilder()
		req.NoError(nb.AssignNode(nd))
		pbn := nb.Build()
		hamtShard, err := hamt.AttemptHAMTShardFromNode(ctx, pbn, &lsys)
		req.NoError(err)

		mi := hamtShard.MapIterator()
		for !mi.Done() {
			k, v, err := mi.Next()
			if nf, ok := err.(interface{ NotFound() bool }); ok && nf.NotFound() {
				// internal shard link that won't load, we don't know what it might
				// point to
				iterNotFound++
				continue
			}
			req.NoError(err)
			ks, err := k.AsString()
			req.NoError(err)
			req.Equal(ipld.Kind_Link, v.Kind())
			lv, err := v.AsLink()
			req.NoError(err)
			iter(dir+"/"+ks, lv)
		}
	}
	// walk the tree
	iter("", cidlink.Link{Cid: carstore.Roots()[0]})

	req.Len(kvs, 1)
	req.Contains(kvs, "/wiki/Cryptographic_hash_function")
	req.Contains(kvs["/wiki/Cryptographic_hash_function"], "<title>Cryptographic hash function</title>\n")
	req.Equal(iterNotFound, 570) // tried to load 570 blocks that were not in the CAR
	req.Len(blockNotFound, 110)  // 110 blocks, for named links, were not found in the CAR
	// some of the root block links
	req.Contains(blockNotFound, "/favicon.ico")
	req.Contains(blockNotFound, "/index.html")
	req.Contains(blockNotFound, "/zimdump_version")
	// some of the shard links
	req.Contains(blockNotFound, "/wiki/UK_railway_Signal")
	req.Contains(blockNotFound, "/wiki/Australian_House")
	req.Contains(blockNotFound, "/wiki/ICloud_Drive")
	req.Contains(blockNotFound, "/wiki/Édouard_Bamberger")
}
