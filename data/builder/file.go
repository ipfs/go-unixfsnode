package builder

import (
	"fmt"
	"io"

	chunk "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/multiformats/go-multicodec"
	multihash "github.com/multiformats/go-multihash/core"

	// raw needed for opening as bytes
	_ "github.com/ipld/go-ipld-prime/codec/raw"
)

type fileShardMeta struct {
	link       datamodel.Link
	byteSize   uint64
	storedSize uint64
}

type fileShards []fileShardMeta

func (fs fileShards) totalByteSize() uint64 {
	var total uint64
	for _, f := range fs {
		total += f.byteSize
	}
	return total
}

func (fs fileShards) totalStoredSize() uint64 {
	var total uint64
	for _, f := range fs {
		total += f.storedSize
	}
	return total
}

func (fs fileShards) byteSizes() []uint64 {
	sizes := make([]uint64, len(fs))
	for i, f := range fs {
		sizes[i] = f.byteSize
	}
	return sizes
}

// BuildUnixFSFile creates a dag of ipld Nodes representing file data.
// This recreates the functionality previously found in
// github.com/ipfs/go-unixfs/importer/balanced, but tailored to the
// go-unixfsnode & ipld-prime data layout of nodes.
// We make some assumptions in building files with this builder to reduce
// complexity, namely:
//   - we assume we are using CIDv1, which has implied that the leaf
//     data nodes are stored as raw bytes.
//     ref: https://github.com/ipfs/go-mfs/blob/1b1fd06cff048caabeddb02d4dbf22d2274c7971/file.go#L50
func BuildUnixFSFile(r io.Reader, chunker string, ls *ipld.LinkSystem) (ipld.Link, uint64, error) {
	src, err := chunk.FromString(r, chunker)
	if err != nil {
		return nil, 0, err
	}

	var prev fileShards
	depth := 1
	for {
		next, err := fileTreeRecursive(depth, prev, src, ls)
		if err != nil {
			return nil, 0, err
		}

		if prev != nil && prev[0].link == next.link {
			if next.link == nil {
				node := basicnode.NewBytes([]byte{})
				link, err := ls.Store(ipld.LinkContext{}, leafLinkProto, node)
				return link, 0, err
			}
			return next.link, next.storedSize, nil
		}

		prev = []fileShardMeta{next}
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

// fileTreeRecursive packs a file into chunks recursively, returning a root for
// this level of recursion, the number of file bytes consumed for this level of
// recursion and and the number of bytes used to store this level of recursion.
func fileTreeRecursive(
	depth int,
	children fileShards,
	src chunk.Splitter,
	ls *ipld.LinkSystem,
) (fileShardMeta, error) {
	if depth == 1 {
		// file leaf, next chunk, encode as raw bytes, store and retuen
		if len(children) > 0 {
			return fileShardMeta{}, fmt.Errorf("leaf nodes cannot have children")
		}
		leaf, err := src.NextBytes()
		if err != nil {
			if err == io.EOF {
				return fileShardMeta{}, nil
			}
			return fileShardMeta{}, err
		}
		node := basicnode.NewBytes(leaf)
		l, sz, err := sizedStore(ls, leafLinkProto, node)
		if err != nil {
			return fileShardMeta{}, err
		}
		return fileShardMeta{link: l, byteSize: uint64(len(leaf)), storedSize: sz}, nil
	}

	// depth > 1

	if children == nil {
		children = make(fileShards, 0)
	}

	// fill up the links for this level, if we need to go beyond
	// DefaultLinksPerBlock we'll end up back here making a parallel tree
	for len(children) < DefaultLinksPerBlock {
		// descend down toward the leaves
		next, err := fileTreeRecursive(depth-1, nil, src, ls)
		if err != nil {
			return fileShardMeta{}, err
		} else if next.link == nil { // eof
			break
		}
		children = append(children, next)
	}

	if len(children) == 0 {
		// empty case
		return fileShardMeta{}, nil
	} else if len(children) == 1 {
		// degenerate case
		return children[0], nil
	}

	// make the unixfs node
	node, err := BuildUnixFS(func(b *Builder) {
		FileSize(b, children.totalByteSize())
		BlockSizes(b, children.byteSizes())
	})
	if err != nil {
		return fileShardMeta{}, err
	}
	pbn, err := packFileChildren(node, children)
	if err != nil {
		return fileShardMeta{}, err
	}

	link, sz, err := sizedStore(ls, fileLinkProto, pbn)
	if err != nil {
		return fileShardMeta{}, err
	}
	return fileShardMeta{
		link:       link,
		byteSize:   children.totalByteSize(),
		storedSize: children.totalStoredSize() + sz,
	}, nil
}

func packFileChildren(node data.UnixFSData, children fileShards) (datamodel.Node, error) {
	dpbb := dagpb.Type.PBNode.NewBuilder()
	pbm, err := dpbb.BeginMap(2)
	if err != nil {
		return nil, err
	}
	pblb, err := pbm.AssembleEntry("Links")
	if err != nil {
		return nil, err
	}
	pbl, err := pblb.BeginList(int64(len(children)))
	if err != nil {
		return nil, err
	}
	for _, c := range children {
		pbln, err := BuildUnixFSDirectoryEntry("", int64(c.storedSize), c.link)
		if err != nil {
			return nil, err
		}
		if err = pbl.AssembleValue().AssignNode(pbln); err != nil {
			return nil, err
		}
	}
	if err = pbl.Finish(); err != nil {
		return nil, err
	}
	if err = pbm.AssembleKey().AssignString("Data"); err != nil {
		return nil, err
	}
	if err = pbm.AssembleValue().AssignBytes(data.EncodeUnixFSData(node)); err != nil {
		return nil, err
	}
	if err = pbm.Finish(); err != nil {
		return nil, err
	}
	return dpbb.Build(), nil
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

// BuildUnixFSSymlink builds a symlink entry in a unixfs tree
func BuildUnixFSSymlink(content string, ls *ipld.LinkSystem) (ipld.Link, uint64, error) {
	// make the unixfs node.
	node, err := BuildUnixFS(func(b *Builder) {
		DataType(b, data.Data_Symlink)
		Data(b, []byte(content))
	})
	if err != nil {
		return nil, 0, err
	}

	dpbb := dagpb.Type.PBNode.NewBuilder()
	pbm, err := dpbb.BeginMap(2)
	if err != nil {
		return nil, 0, err
	}
	pblb, err := pbm.AssembleEntry("Links")
	if err != nil {
		return nil, 0, err
	}
	pbl, err := pblb.BeginList(0)
	if err != nil {
		return nil, 0, err
	}
	if err = pbl.Finish(); err != nil {
		return nil, 0, err
	}
	if err = pbm.AssembleKey().AssignString("Data"); err != nil {
		return nil, 0, err
	}
	if err = pbm.AssembleValue().AssignBytes(data.EncodeUnixFSData(node)); err != nil {
		return nil, 0, err
	}
	if err = pbm.Finish(); err != nil {
		return nil, 0, err
	}
	pbn := dpbb.Build()

	return sizedStore(ls, fileLinkProto, pbn)
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
//   - the expected distribution of block sizes
//   - the expected distribution of link sizes
//   - desired access speed
//
// For now, we use:
//
//	var roughLinkBlockSize = 1 << 13 // 8KB
//	var roughLinkSize = 34 + 8 + 5   // sha256 multihash + size + no name
//	                                 // + protobuf framing
//	var DefaultLinksPerBlock = (roughLinkBlockSize / roughLinkSize)
//	                         = ( 8192 / 47 )
//	                         = (approximately) 174
var DefaultLinksPerBlock = roughLinkBlockSize / roughLinkSize
