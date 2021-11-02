package file

import (
	"context"
	"fmt"
	"io"

	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
)

// NewUnixFSFile attempts to construct an ipld node from the base protobuf node representing the
// root of a unixfs File.
// It provides a `bytes` view over the file, along with access to io.Reader streaming access
// to file data.
func NewUnixFSFile(ctx context.Context, substrate ipld.Node, lsys *ipld.LinkSystem) (StreamableByteNode, error) {
	if substrate.Kind() == ipld.Kind_Bytes {
		// A raw / single-node file.
		return &singleNodeFile{substrate, 0}, nil
	}
	return &shardNodeFile{ctx, lsys, substrate, nil}, nil
}

// A StreamableByteNode is an ipld.Node that can be streamed over. It is guaranteed to have a Bytes type.
type StreamableByteNode interface {
	ipld.Node
	io.Reader
}

type singleNodeFile struct {
	ipld.Node
	ptr int
}

func (f *singleNodeFile) Read(p []byte) (int, error) {
	buf, err := f.Node.AsBytes()
	if err != nil {
		return 0, err
	}
	if f.ptr >= len(buf) {
		return 0, io.EOF
	}
	n := copy(p, buf[f.ptr:])
	f.ptr += n
	return n, nil
}

type shardNodeFile struct {
	ctx       context.Context
	lsys      *ipld.LinkSystem
	substrate ipld.Node
	rdr       io.Reader
}

var _ ipld.Node = (*shardNodeFile)(nil)

func (s *shardNodeFile) Read(p []byte) (int, error) {
	// collect the sub-nodes on first use
	if s.rdr == nil {
		links, err := s.substrate.LookupByString("Links")
		if err != nil {
			return 0, err
		}
		readers := make([]io.Reader, 0)
		lnki := links.ListIterator()
		for !lnki.Done() {
			_, lnk, err := lnki.Next()
			if err != nil {
				return 0, err
			}
			if pbl, ok := lnk.(dagpb.PBLink); ok {
				target, err := s.lsys.Load(ipld.LinkContext{Ctx: s.ctx}, pbl.Hash.Link(), basicnode.Prototype.Any)
				if err != nil {
					return 0, err
				}

				asFSNode, err := NewUnixFSFile(s.ctx, target, s.lsys)
				if err != nil {
					return 0, err
				}
				readers = append(readers, asFSNode)
			} else {
				return 0, fmt.Errorf("unsupported link type: %T", lnk)
			}
		}
		s.rdr = io.MultiReader(readers...)
	}
	return s.rdr.Read(p)
}

func (s *shardNodeFile) Kind() ipld.Kind {
	return ipld.Kind_Bytes
}

func (s *shardNodeFile) AsBytes() ([]byte, error) {
	return io.ReadAll(s)
}

func (s *shardNodeFile) AsBool() (bool, error) {
	return false, ipld.ErrWrongKind{TypeName: "bool", MethodName: "AsBool", AppropriateKind: ipld.KindSet_JustBytes}
}

func (s *shardNodeFile) AsInt() (int64, error) {
	return 0, ipld.ErrWrongKind{TypeName: "int", MethodName: "AsInt", AppropriateKind: ipld.KindSet_JustBytes}
}

func (s *shardNodeFile) AsFloat() (float64, error) {
	return 0, ipld.ErrWrongKind{TypeName: "float", MethodName: "AsFloat", AppropriateKind: ipld.KindSet_JustBytes}
}

func (s *shardNodeFile) AsString() (string, error) {
	return "", ipld.ErrWrongKind{TypeName: "string", MethodName: "AsString", AppropriateKind: ipld.KindSet_JustBytes}
}

func (s *shardNodeFile) AsLink() (ipld.Link, error) {
	return nil, ipld.ErrWrongKind{TypeName: "link", MethodName: "AsLink", AppropriateKind: ipld.KindSet_JustBytes}
}

func (s *shardNodeFile) AsNode() (ipld.Node, error) {
	return nil, nil
}

func (s *shardNodeFile) Size() int {
	return 0
}

func (s *shardNodeFile) IsAbsent() bool {
	return false
}

func (s *shardNodeFile) IsNull() bool {
	return s.substrate.IsNull()
}

func (s *shardNodeFile) Length() int64 {
	return 0
}

func (s *shardNodeFile) ListIterator() ipld.ListIterator {
	return nil
}

func (s *shardNodeFile) MapIterator() ipld.MapIterator {
	return nil
}

func (s *shardNodeFile) LookupByIndex(idx int64) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{}
}

func (s *shardNodeFile) LookupByString(key string) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{}
}

func (s *shardNodeFile) LookupByNode(key ipld.Node) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{}
}

func (s *shardNodeFile) LookupBySegment(seg ipld.PathSegment) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{}
}

func (s *shardNodeFile) Prototype() ipld.NodePrototype {
	return basicnode.Prototype__Bytes{}
}
