package file

import (
	"context"
	"io"

	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
)

func newDeferredFileNode(ctx context.Context, lsys *ipld.LinkSystem, root ipld.Link) StreamableByteNode {
	dfn := deferredFileNode{
		StreamableByteNode: nil,
		root:               root,
		l:                  lsys,
		ctx:                ctx,
	}
	dfn.StreamableByteNode = deferred{&dfn}
	return &dfn
}

type deferredFileNode struct {
	StreamableByteNode

	root ipld.Link
	l    *ipld.LinkSystem
	ctx  context.Context
}

func (d *deferredFileNode) resolve() error {
	target, err := d.l.Load(ipld.LinkContext{Ctx: d.ctx}, d.root, protoFor(d.root))
	if err != nil {
		return err
	}

	asFSNode, err := NewUnixFSFile(d.ctx, target, d.l)
	if err != nil {
		return err
	}
	d.StreamableByteNode = asFSNode
	d.root = nil
	d.l = nil
	d.ctx = nil
	return nil
}

type deferred struct {
	*deferredFileNode
}

func (d deferred) AsLargeBytes() (io.ReadSeeker, error) {
	if err := d.deferredFileNode.resolve(); err != nil {
		return nil, err
	}
	return d.deferredFileNode.AsLargeBytes()
}

func (d deferred) Read(p []byte) (int, error) {
	if err := d.deferredFileNode.resolve(); err != nil {
		return 0, err
	}
	return d.deferredFileNode.Read(p)
}

func (d deferred) Seek(offset int64, whence int) (int64, error) {
	if err := d.deferredFileNode.resolve(); err != nil {
		return 0, err
	}
	return d.deferredFileNode.Seek(offset, whence)
}

func (d deferred) Kind() ipld.Kind {
	return ipld.Kind_Bytes
}

func (d deferred) AsBytes() ([]byte, error) {
	if err := d.deferredFileNode.resolve(); err != nil {
		return []byte{}, err
	}

	return d.deferredFileNode.AsBytes()
}

func (d deferred) AsBool() (bool, error) {
	return false, ipld.ErrWrongKind{TypeName: "bool", MethodName: "AsBool", AppropriateKind: ipld.KindSet_JustBytes}
}

func (d deferred) AsInt() (int64, error) {
	return 0, ipld.ErrWrongKind{TypeName: "int", MethodName: "AsInt", AppropriateKind: ipld.KindSet_JustBytes}
}

func (d deferred) AsFloat() (float64, error) {
	return 0, ipld.ErrWrongKind{TypeName: "float", MethodName: "AsFloat", AppropriateKind: ipld.KindSet_JustBytes}
}

func (d deferred) AsString() (string, error) {
	return "", ipld.ErrWrongKind{TypeName: "string", MethodName: "AsString", AppropriateKind: ipld.KindSet_JustBytes}
}

func (d deferred) AsLink() (ipld.Link, error) {
	return nil, ipld.ErrWrongKind{TypeName: "link", MethodName: "AsLink", AppropriateKind: ipld.KindSet_JustBytes}
}

func (d deferred) AsNode() (ipld.Node, error) {
	return nil, nil
}

func (d deferred) Size() int {
	return 0
}

func (d deferred) IsAbsent() bool {
	return false
}

func (d deferred) IsNull() bool {
	if err := d.deferredFileNode.resolve(); err != nil {
		return true
	}
	return d.deferredFileNode.IsNull()
}

func (d deferred) Length() int64 {
	return 0
}

func (d deferred) ListIterator() ipld.ListIterator {
	return nil
}

func (d deferred) MapIterator() ipld.MapIterator {
	return nil
}

func (d deferred) LookupByIndex(idx int64) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{}
}

func (d deferred) LookupByString(key string) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{}
}

func (d deferred) LookupByNode(key ipld.Node) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{}
}

func (d deferred) LookupBySegment(seg ipld.PathSegment) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{}
}

// shardded files / nodes look like dagpb nodes.
func (d deferred) Prototype() ipld.NodePrototype {
	return dagpb.Type.PBNode
}
