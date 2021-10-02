package builder

import (
	"fmt"
	"os"
	"path"

	"github.com/ipfs/go-unixfsnode/data"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
)

// BuildUnixFSRecursive returns a link pointing to the UnixFS node representing
// the file or directory tree pointed to by `root`
// TODO: support symlinks
func BuildUnixFSRecursive(root string, ls *ipld.LinkSystem) (ipld.Link, uint64, error) {
	info, err := os.Stat(root)
	if err != nil {
		return nil, 0, err
	}

	if info.IsDir() {
		entries, err := os.ReadDir(root)
		if err != nil {
			return nil, 0, err
		}
		lnks := make([]dagpb.PBLink, 0, len(entries))
		for _, e := range entries {
			lnk, sz, err := BuildUnixFSRecursive(path.Join(root, e.Name()), ls)
			if err != nil {
				return nil, 0, err
			}
			entry, err := BuildUnixFSDirectoryEntry(e.Name(), int64(sz), lnk)
			if err != nil {
				return nil, 0, err
			}
			lnks = append(lnks, entry)
		}
		outLnk, err := BuildUnixFSDirectory(lnks, ls)
		return outLnk, 0, err
	}
	// else: file
	fp, err := os.Open(root)
	if err != nil {
		return nil, 0, err
	}
	defer fp.Close()
	return BuildUnixFSFile(fp, "", ls)
}

// BuildUnixFSDirectory creates a directory link over a collection of entries.
// TODO: support sharded directories.
func BuildUnixFSDirectory(entries []dagpb.PBLink, ls *ipld.LinkSystem) (ipld.Link, error) {
	if len(entries) > DefaultLinksPerBlock {
		return nil, fmt.Errorf("this builder does support sharded directories")
	}
	ufd, err := BuildUnixFS(func(b *Builder) {
		DataType(b, data.Data_Directory)
	})
	if err != nil {
		return nil, err
	}
	pbb := dagpb.Type.PBNode.NewBuilder()
	pbm, err := pbb.BeginMap(2)
	if err != nil {
		return nil, err
	}
	pbm.AssembleKey().AssignString("Data")
	pbm.AssembleValue().AssignBytes(data.EncodeUnixFSData(ufd))
	pbm.AssembleKey().AssignString("Links")
	lnkBuilder := dagpb.Type.PBLinks.NewBuilder()
	lnks, err := lnkBuilder.BeginList(int64(len(entries)))
	if err != nil {
		return nil, err
	}
	// sorting happens in codec-dagpb
	for _, e := range entries {
		if err := lnks.AssembleValue().AssignNode(e); err != nil {
			return nil, err
		}
	}
	if err := lnks.Finish(); err != nil {
		return nil, err
	}
	pbm.AssembleValue().AssignNode(lnkBuilder.Build())
	if err := pbm.Finish(); err != nil {
		return nil, err
	}
	node := pbb.Build()
	return ls.Store(ipld.LinkContext{}, fileLinkProto, node)
}
