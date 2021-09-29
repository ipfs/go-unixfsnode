package builder

import (
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	chunk "github.com/ipfs/go-ipfs-chunker"
	"github.com/ipfs/go-unixfsnode/data"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/multiformats/go-multicodec"
	multihash "github.com/multiformats/go-multihash/core"
)

// BuildUnixFSFile creates a dag of ipld Nodes representing file data.
// This recreates the functionality previously found in
// github.com/ipfs/go-unixfs/importer/balanced, but tailored to the
// go-unixfsnode & ipld-prime data layout of nodes.
// We make some assumptions in building files with this builder to reduce
// complexity, namely:
// * we assume we are using CIDv1, which has implied that the leaf
//   data nodes are stored as raw bytes.
//   ref: https://github.com/ipfs/go-mfs/blob/1b1fd06cff048caabeddb02d4dbf22d2274c7971/file.go#L50
func BuildUnixFSFile(r io.Reader, chunker string, ls *ipld.LinkSystem) (ipld.Link, uint64, error) {
	s, err := chunk.FromString(r, chunker)
	if err != nil {
		return nil, 0, err
	}

	var prev []ipld.Link
	var prevLen []uint64
	depth := 1
	for {
		root, size, err := treeRecursive(depth, prev[:], prevLen[:], s, ls)
		if err != nil {
			return nil, 0, err
		}

		if prev != nil && prev[0] == root {
			return root, size, nil
		}

		prev = []ipld.Link{root}
		prevLen = []uint64{size}
		depth++
	}
}

var fileLinkProto = cidlink.LinkPrototype{
	Prefix: cid.Prefix{
		Version:  1,
		Codec:    uint64(multicodec.DagPb),
		MhType:   multihash.SHA2_256,
		MhLength: 32,
	},
}

var leafLinkProto = cidlink.LinkPrototype{
	Prefix: cid.Prefix{
		Version:  1,
		Codec:    uint64(multicodec.Raw),
		MhType:   multihash.SHA2_256,
		MhLength: 32,
	},
}

func treeRecursive(depth int, children []ipld.Link, childLen []uint64, src chunk.Splitter, ls *ipld.LinkSystem) (ipld.Link, uint64, error) {
	if depth == 1 && len(children) > 0 {
		return nil, 0, fmt.Errorf("leaf nodes cannot have children")
	} else if depth == 1 {
		leaf, err := src.NextBytes()
		if err == io.EOF {
			return nil, 0, nil
		} else if err != nil {
			return nil, 0, err
		}
		node := basicnode.NewBytes(leaf)
		link, err := ls.Store(ipld.LinkContext{}, leafLinkProto, node)
		return link, uint64(len(leaf)), err
	}
	// depth > 1.
	totalSize := uint64(0)
	blksizes := make([]uint64, 0, DefaultLinksPerBlock)
	if children == nil {
		children = make([]ipld.Link, 0)
	} else {
		for i := range children {
			blksizes = append(blksizes, childLen[i])
			totalSize += childLen[i]
		}
	}
	for len(children) < DefaultLinksPerBlock {
		nxt, sz, err := treeRecursive(depth-1, nil, nil, src, ls)
		if err != nil {
			return nil, 0, err
		} else if nxt == nil {
			// eof
			break
		}
		totalSize += sz
		children = append(children, nxt)
		blksizes = append(blksizes, sz)
	}
	if len(children) == 0 {
		// empty case.
		return nil, 0, nil
	} else if len(children) == 1 {
		// degenerate case
		return children[0], childLen[0], nil
	}

	// make the unixfs node.
	node, err := BuildUnixFS(func(b *Builder) {
		FileSize(b, totalSize)
		BlockSizes(b, blksizes)
	})
	if err != nil {
		return nil, 0, err
	}

	// Pack into the dagpb node.
	dpbb := dagpb.Type.PBNode.NewBuilder()
	pbm, _ := dpbb.BeginMap(2)
	pblb, _ := pbm.AssembleEntry("Links")
	pbl, _ := pblb.BeginList(int64(len(children)))
	for i, c := range children {
		pbln, err := BuildUnixFSDirectoryEntry("", int64(blksizes[i]), c)
		if err != nil {
			return nil, 0, err
		}
		_ = pbl.AssembleValue().AssignNode(pbln)
	}
	_ = pbl.Finish()
	_ = pbm.AssembleKey().AssignString("Data")
	_ = pbm.AssembleValue().AssignBytes(data.EncodeUnixFSData(node))
	_ = pbm.Finish()
	pbn := dpbb.Build()

	link, err := ls.Store(ipld.LinkContext{}, fileLinkProto, pbn)
	// calculate the dagpb node's size and add as overhead.
	cl, ok := link.(cidlink.Link)
	if !ok {
		return nil, 0, fmt.Errorf("unexpected non-cid linksystem")
	}
	rawlnk := cid.NewCidV1(uint64(multicodec.Raw), cl.Cid.Hash())
	rn, err := ls.Load(ipld.LinkContext{}, cidlink.Link{Cid: rawlnk}, basicnode.Prototype__Bytes{})
	if err != nil {
		return nil, 0, fmt.Errorf("could not re-interpret dagpb node as bytes")
	}
	rnb, err := rn.AsBytes()
	if err != nil {
		return nil, 0, fmt.Errorf("could not re-interpret dagpb node as bytes")
	}
	return link, totalSize + uint64(len(rnb)), err
}

// BuildUnixFSDirectoryEntry creates the link to a file or directory as it appears within a unixfs directory.
func BuildUnixFSDirectoryEntry(name string, size int64, hash ipld.Link) (dagpb.PBLink, error) {
	dpbl := dagpb.Type.PBLink.NewBuilder()
	lma, err := dpbl.BeginMap(3)
	if err != nil {
		return nil, err
	}
	if err = lma.AssembleKey().AssignString("Hash"); err != nil {
		return nil, err
	}
	if err = lma.AssembleValue().AssignLink(hash); err != nil {
		return nil, err
	}
	if err = lma.AssembleKey().AssignString("Name"); err != nil {
		return nil, err
	}
	if err = lma.AssembleValue().AssignString(name); err != nil {
		return nil, err
	}
	if err = lma.AssembleKey().AssignString("Tsize"); err != nil {
		return nil, err
	}
	if err = lma.AssembleValue().AssignInt(size); err != nil {
		return nil, err
	}
	if err = lma.Finish(); err != nil {
		return nil, err
	}
	return dpbl.Build().(dagpb.PBLink), nil
}

// Constants below are from
// https://github.com/ipfs/go-unixfs/blob/ec6bb5a4c5efdc3a5bce99151b294f663ee9c08d/importer/helpers/helpers.go

// BlockSizeLimit specifies the maximum size an imported block can have.
var BlockSizeLimit = 1048576 // 1 MB

// rough estimates on expected sizes
var roughLinkBlockSize = 1 << 13 // 8KB
var roughLinkSize = 34 + 8 + 5   // sha256 multihash + size + no name + protobuf framing

// DefaultLinksPerBlock governs how the importer decides how many links there
// will be per block. This calculation is based on expected distributions of:
//  * the expected distribution of block sizes
//  * the expected distribution of link sizes
//  * desired access speed
// For now, we use:
//
//   var roughLinkBlockSize = 1 << 13 // 8KB
//   var roughLinkSize = 34 + 8 + 5   // sha256 multihash + size + no name
//                                    // + protobuf framing
//   var DefaultLinksPerBlock = (roughLinkBlockSize / roughLinkSize)
//                            = ( 8192 / 47 )
//                            = (approximately) 174
var DefaultLinksPerBlock = roughLinkBlockSize / roughLinkSize
