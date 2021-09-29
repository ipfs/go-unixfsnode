package builder

import (
	"fmt"
	"sort"

	"github.com/ipfs/go-unixfsnode/data"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
)

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
	sort.Stable(LinkSlice(entries))
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

type LinkSlice []dagpb.PBLink

func (ls LinkSlice) Len() int      { return len(ls) }
func (ls LinkSlice) Swap(a, b int) { ls[a], ls[b] = ls[b], ls[a] }
func (ls LinkSlice) Less(a, b int) bool {
	na := ""
	if ls[a].Name.Exists() {
		na, _ = ls[a].Name.Must().AsString()
	}
	nb := ""
	if ls[b].Name.Exists() {
		nb, _ = ls[b].Name.Must().AsString()
	}
	return na < nb
}
