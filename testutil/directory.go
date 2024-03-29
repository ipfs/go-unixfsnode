package testutil

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/stretchr/testify/require"
)

// DirEntry represents a flattened directory entry, where Path is from the
// root of the directory and Content is the file contents. It is intended
// that a DirEntry slice can be used to represent a full-depth directory without
// needing nesting.
type DirEntry struct {
	Path     string
	Content  []byte
	Root     cid.Cid
	SelfCids []cid.Cid
	TSize    uint64
	Children []DirEntry
}

func (de DirEntry) Size() (int64, error) {
	return int64(de.TSize), nil
}

func (de DirEntry) Link() ipld.Link {
	return cidlink.Link{Cid: de.Root}
}

// ToDirEntry takes a LinkSystem containing UnixFS data and builds a DirEntry
// tree representing the file and directory structure it finds starting at the
// rootCid. If expectFull is true, it will error if it encounters a UnixFS
// node that it cannot fully load. If expectFull is false, it will ignore
// errors and return nil for any node it cannot load.
func ToDirEntry(t *testing.T, linkSys linking.LinkSystem, rootCid cid.Cid, expectFull bool) DirEntry {
	return ToDirEntryFrom(t, linkSys, rootCid, "", expectFull)
}

// ToDirEntryFrom is the same as ToDirEntry but allows specifying a rootPath
// such that the resulting DirEntry tree will all have that path as a prefix.
// This is useful when representing a sub-DAG of a larger DAG where you want
// to make direct comparisons.
func ToDirEntryFrom(t *testing.T, linkSys linking.LinkSystem, rootCid cid.Cid, rootPath string, expectFull bool) DirEntry {
	var proto datamodel.NodePrototype = dagpb.Type.PBNode
	isDagPb := rootCid.Prefix().Codec == cid.DagProtobuf
	if !isDagPb {
		proto = basicnode.Prototype.Any
	}
	node, err := linkSys.Load(linking.LinkContext{Ctx: context.TODO()}, cidlink.Link{Cid: rootCid}, proto)
	if expectFull {
		require.NoError(t, err)
	} else if err != nil {
		if e, ok := err.(interface{ NotFound() bool }); ok && e.NotFound() {
			return DirEntry{}
		}
		require.NoError(t, err)
	}

	if node.Kind() == ipld.Kind_Bytes { // is a file
		byts, err := node.AsBytes()
		require.NoError(t, err)
		return DirEntry{
			Path:    rootPath,
			Content: byts,
			Root:    rootCid,
		}
	}

	children := make([]DirEntry, 0)
	if isDagPb {
		// else is likely a directory
		for itr := node.MapIterator(); !itr.Done(); {
			k, v, err := itr.Next()
			require.NoError(t, err)
			childName, err := k.AsString()
			require.NoError(t, err)
			childLink, err := v.AsLink()
			require.NoError(t, err)
			child := ToDirEntryFrom(t, linkSys, childLink.(cidlink.Link).Cid, rootPath+"/"+childName, expectFull)
			children = append(children, child)
		}
	} else {
		// not a dag-pb node, let's pretend it is but using IPLD pathing rules
		err := traversal.WalkLocal(node, func(prog traversal.Progress, n ipld.Node) error {
			if n.Kind() == ipld.Kind_Link {
				l, err := n.AsLink()
				if err != nil {
					return err
				}
				child := ToDirEntryFrom(t, linkSys, l.(cidlink.Link).Cid, rootPath+"/"+prog.Path.String(), expectFull)
				children = append(children, child)
			}
			return nil
		})
		require.NoError(t, err)
	}

	return DirEntry{
		Path:     rootPath,
		Root:     rootCid,
		Children: children,
	}
}

// CompareDirEntries is a safe, recursive comparison between two DirEntry
// values. It doesn't strictly require child ordering to match, but it does
// require that all children exist and match, in some order.
func CompareDirEntries(t *testing.T, a, b DirEntry) {
	// t.Log("CompareDirEntries", a.Path, b.Path) // TODO: remove this
	require.Equal(t, a.Path, b.Path)
	require.Equal(t, a.Root.String(), b.Root.String(), a.Path+" root mismatch")
	hashA := sha256.Sum256(a.Content)
	hashB := sha256.Sum256(b.Content)
	require.Equal(t, hex.EncodeToString(hashA[:]), hex.EncodeToString(hashB[:]), a.Path+"content hash mismatch")
	require.Equal(t, len(a.Children), len(b.Children), fmt.Sprintf("%s child length mismatch %d <> %d", a.Path, len(a.Children), len(b.Children)))
	for i := range a.Children {
		// not necessarily in order
		var found bool
		for j := range b.Children {
			if a.Children[i].Path == b.Children[j].Path {
				found = true
				CompareDirEntries(t, a.Children[i], b.Children[j])
			}
		}
		require.True(t, found, fmt.Sprintf("@ path [%s], a's child [%s] not found in b", a.Path, a.Children[i].Path))
	}
}

// WrapContent embeds the content we want in some random nested content such
// that it's fetchable under the provided path. If exclusive is true, the
// content will be the only thing under the path. If false, there will be
// content before and after the wrapped content at each point in the path.
func WrapContent(t *testing.T, rndReader io.Reader, lsys *ipld.LinkSystem, content DirEntry, wrapPath string, exclusive bool) DirEntry {
	want := content
	ps := datamodel.ParsePath(wrapPath)
	for ps.Len() > 0 {
		de := []DirEntry{}
		if !exclusive {
			before := GenerateDirectory(t, lsys, rndReader, 4<<10, false)
			before.Path = "!before"
			de = append(de, before)
		}
		want.Path = ps.Last().String()
		de = append(de, want)
		if !exclusive {
			after := GenerateDirectory(t, lsys, rndReader, 4<<11, true)
			after.Path = "~after"
			de = append(de, after)
		}
		want = BuildDirectory(t, lsys, de, false)
		ps = ps.Pop()
	}
	return want
}
