package builder

import (
	"fmt"

	bitfield "github.com/ipfs/go-bitfield"
	"github.com/ipfs/go-unixfsnode/data"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/multiformats/go-multihash"
)

type shard struct {
	// metadata about the shard
	hasher uint64
	size   int
	width  int
	depth  int

	children map[int]entry
}

// a shard entry is either another shard, or a direct link.
type entry struct {
	*shard
	*hamtLink
}

// a hamtLink is a member of the hamt - the file/directory pointed to, but
// stored with it's hashed key used for addressing.
type hamtLink struct {
	hash hashBits
	dagpb.PBLink
}

// BuildUnixFSShardedDirectory will build a hamt of unixfs hamt shards encoing a directory with more entries
// than is typically allowed to fit in a standard IPFS single-block unixFS directory.
func BuildUnixFSShardedDirectory(size int, hasher uint64, entries []dagpb.PBLink, ls *ipld.LinkSystem) (ipld.Link, error) {
	// hash the entries
	h, err := multihash.GetHasher(hasher)
	if err != nil {
		return nil, err
	}
	hamtEntries := make([]hamtLink, 0, len(entries))
	for _, e := range entries {
		name := e.Name.Must().String()
		sum := h.Sum([]byte(name))
		hamtEntries = append(hamtEntries, hamtLink{
			sum,
			e,
		})
	}

	sharder := shard{
		hasher: hasher,
		size:   size,
		width:  len(fmt.Sprintf("%X", size-1)),
		depth:  0,

		children: make(map[int]entry),
	}

	for _, entry := range hamtEntries {
		sharder.add(entry)
	}

	return sharder.serialize(ls)
}

func (s *shard) add(lnk hamtLink) error {
	// get the bucket for lnk
	bucket, err := lnk.hash.Slice(s.depth*s.size, s.size)
	if err != nil {
		return err
	}

	current, ok := s.children[bucket]
	if !ok {
		s.children[bucket] = entry{nil, &lnk}
		return nil
	} else if current.shard != nil {
		return current.shard.add(lnk)
	}
	// make a shard for current and lnk
	newShard := entry{
		&shard{
			hasher:   s.hasher,
			size:     s.size,
			width:    s.width,
			depth:    s.depth + 1,
			children: make(map[int]entry),
		},
		nil,
	}
	if err := newShard.add(*current.hamtLink); err != nil {
		return err
	}
	s.children[bucket] = newShard
	return newShard.add(lnk)
}

func (s *shard) formatLinkName(name string, idx int) string {
	return fmt.Sprintf("%*X%s", s.width, idx, name)
}

// bitmap calculates the bitmap of which links in the shard are set.
func (s *shard) bitmap() []byte {
	bm := bitfield.NewBitfield(s.size)
	for i := 0; i < s.size; i++ {
		if _, ok := s.children[i]; ok {
			bm.SetBit(i)
		}
	}
	return bm.Bytes()
}

// serialize stores the concrete representation of this shard in the link system and
// returns a link to it.
func (s *shard) serialize(ls *ipld.LinkSystem) (ipld.Link, error) {
	ufd, err := BuildUnixFS(func(b *Builder) {
		DataType(b, data.Data_HAMTShard)
		HashType(b, s.hasher)
		Data(b, s.bitmap())
		Fanout(b, uint64(s.size))
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
	lnks, err := lnkBuilder.BeginList(int64(len(s.children)))
	if err != nil {
		return nil, err
	}
	// sorting happens in codec-dagpb
	for idx, e := range s.children {
		var lnk dagpb.PBLink
		if e.shard != nil {
			ipldLnk, err := e.shard.serialize(ls)
			if err != nil {
				return nil, err
			}
			fullName := s.formatLinkName("", idx)
			lnk, err = BuildUnixFSDirectoryEntry(fullName, 0, ipldLnk)
			if err != nil {
				return nil, err
			}
		} else {
			fullName := s.formatLinkName(e.Name.Must().String(), idx)
			lnk, err = BuildUnixFSDirectoryEntry(fullName, e.Tsize.Must().Int(), e.Hash.Link())
		}
		if err != nil {
			return nil, err
		}
		if err := lnks.AssembleValue().AssignNode(lnk); err != nil {
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
