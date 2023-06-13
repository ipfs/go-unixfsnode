package iter

import (
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
)

// pbLinkItr behaves like an list of links iterator, even thought the HAMT behavior is more complicated
type pbLinkItr interface {
	Next() (int64, dagpb.PBLink, error)
	Done() bool
}

type TransformNameFunc func(dagpb.String) dagpb.String

func NewUnixFSDirMapIterator(itr pbLinkItr, transformName TransformNameFunc) ipld.MapIterator {
	return &UnixFSDir__MapItr{itr, transformName}
}

// UnixFSDir__MapItr throught the links as if they were a map
// Note: for now it does return links with no name, where the key is just String("")
type UnixFSDir__MapItr struct {
	_substrate    pbLinkItr
	transformName TransformNameFunc
}

func (itr *UnixFSDir__MapItr) Next() (k ipld.Node, v ipld.Node, err error) {
	_, next, err := itr._substrate.Next()
	if err != nil {
		return nil, nil, err
	}
	if next == nil {
		return nil, nil, ipld.ErrIteratorOverread{}
	}
	if next.FieldName().Exists() {
		name := next.FieldName().Must()
		if itr.transformName != nil {
			name = itr.transformName(name)
		}
		return name, next.FieldHash(), nil
	}
	nb := dagpb.Type.String.NewBuilder()
	err = nb.AssignString("")
	if err != nil {
		return nil, nil, err
	}
	s := nb.Build()
	return s, next.FieldHash(), nil
}

func (itr *UnixFSDir__MapItr) Done() bool {
	return itr._substrate.Done()
}

func NewUnixFSDirListIterator(itr pbLinkItr, transformName TransformNameFunc) ipld.ListIterator {
	return &UnixFSDir__ListItr{itr, transformName}
}

func (itr *UnixFSDir__ListItr) Next() (i int64, v ipld.Node, err error) {
	i, next, err := itr._substrate.Next()
	if err != nil {
		return i, nil, err
	}
	if next == nil {
		return i, nil, ipld.ErrIteratorOverread{}
	}
	if next.FieldName().Exists() {
		name := next.FieldName().Must()
		if itr.transformName != nil {
			name = itr.transformName(name)
		}
		nd, err := qp.BuildMap(dagpb.Type.PBLink, 3, func(ma datamodel.MapAssembler) {
			qp.MapEntry(ma, "Name", qp.Node(name))
			if next.FieldTsize().Exists() {
				qp.MapEntry(ma, "Tsize", qp.Node(next.FieldTsize().Must()))
			}
			qp.MapEntry(ma, "Hash", qp.Node(next.FieldHash()))
		})
		if err != nil {
			return i, nil, err
		}
		return i, nd, nil
	}
	return i, next, nil
}

func (itr *UnixFSDir__ListItr) Done() bool {
	return itr._substrate.Done()
}

type UnixFSDir__ListItr struct {
	_substrate    pbLinkItr
	transformName TransformNameFunc
}

type UnixFSDir__Itr struct {
	_substrate    pbLinkItr
	transformName TransformNameFunc
}

func NewUnixFSDirIterator(itr pbLinkItr, transformName TransformNameFunc) *UnixFSDir__Itr {
	return &UnixFSDir__Itr{itr, transformName}
}
func (itr *UnixFSDir__Itr) Next() (k dagpb.String, v dagpb.Link) {
	_, next, err := itr._substrate.Next()
	if err != nil {
		return nil, nil
	}
	if next == nil {
		return nil, nil
	}
	if next.FieldName().Exists() {
		name := next.FieldName().Must()
		if itr.transformName != nil {
			name = itr.transformName(name)
		}
		return name, next.FieldHash()
	}
	nb := dagpb.Type.String.NewBuilder()
	err = nb.AssignString("")
	if err != nil {
		return nil, nil
	}
	s := nb.Build()
	return s.(dagpb.String), next.FieldHash()
}

func (itr *UnixFSDir__Itr) Done() bool {
	return itr._substrate.Done()
}
