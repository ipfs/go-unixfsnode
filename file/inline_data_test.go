package file_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipfs/go-unixfsnode/data"
	"github.com/ipfs/go-unixfsnode/data/builder"
	"github.com/ipfs/go-unixfsnode/file"

	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	multihash "github.com/multiformats/go-multihash/core"
)

type inlineTestFile struct {
	data           []byte
	children       []inlineTestFile
	raw            bool
	omitFileSize   bool
	omitBlockSizes bool
}

func storeInlineTestFile(t *testing.T, ls *ipld.LinkSystem, f inlineTestFile) (ipld.Link, int64, int64) {
	t.Helper()
	proto := cidlink.LinkPrototype{
		Prefix: cid.Prefix{Version: 1, Codec: cid.Raw, MhType: multihash.SHA2_256, MhLength: 32},
	}
	n := ipld.Node(basicnode.NewBytes(f.data))
	size := int64(len(f.data))
	storedChildren := int64(0)
	if !f.raw {
		proto.Prefix.Codec = cid.DagProtobuf
		links := make([]ipld.Link, len(f.children))
		blockSizes := make([]uint64, len(f.children))
		storedSizes := make([]int64, len(f.children))
		for i, child := range f.children {
			link, childSize, storedSize := storeInlineTestFile(t, ls, child)
			links[i], blockSizes[i], storedSizes[i] = link, uint64(childSize), storedSize
			size += childSize
			storedChildren += storedSize
		}
		md, err := builder.BuildUnixFS(func(b *builder.Builder) {
			if f.data != nil {
				builder.Data(b, f.data)
			}
			if !f.omitFileSize {
				builder.FileSize(b, uint64(size))
			}
			if !f.omitBlockSizes {
				builder.BlockSizes(b, blockSizes)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		n, err = qp.BuildMap(dagpb.Type.PBNode, 2, func(ma ipld.MapAssembler) {
			qp.MapEntry(ma, "Data", qp.Bytes(data.EncodeUnixFSData(md)))
			qp.MapEntry(ma, "Links", qp.List(int64(len(links)), func(la ipld.ListAssembler) {
				for i, link := range links {
					qp.ListEntry(la, qp.Map(2, func(ma ipld.MapAssembler) {
						qp.MapEntry(ma, "Hash", qp.Link(link))
						qp.MapEntry(ma, "Tsize", qp.Int(storedSizes[i]))
					}))
				}
			}))
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	link, err := ls.Store(ipld.LinkContext{}, proto, n)
	if err != nil {
		t.Fatal(err)
	}
	r, err := ls.StorageReadOpener(ipld.LinkContext{}, link)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return link, size, int64(len(encoded)) + storedChildren
}

func TestInlineDataWithLinks(t *testing.T) {
	raw := func(s string) inlineTestFile { return inlineTestFile{data: []byte(s), raw: true} }
	for _, tc := range []struct {
		name string
		file inlineTestFile
		want string
	}{
		{"raw children", inlineTestFile{data: []byte("hello "), children: []inlineTestFile{raw("world"), raw("!")}}, "hello world!"},
		{"no inline data", inlineTestFile{children: []inlineTestFile{raw("world"), raw("!")}}, "world!"},
		{"empty inline data", inlineTestFile{data: []byte{}, children: []inlineTestFile{raw("world"), raw("!")}}, "world!"},
		{"nested inline data", inlineTestFile{data: []byte("head:"), children: []inlineTestFile{
			{data: []byte("middle:"), children: []inlineTestFile{raw("tail"), {data: []byte("!")}}},
			raw("?"),
		}}, "head:middle:tail!?"},
		{"empty child", inlineTestFile{data: []byte("prefix"), children: []inlineTestFile{raw("")}}, "prefix"},
		{"wrapped leaf", inlineTestFile{data: []byte("leaf")}, "leaf"},
		{"binary inline data", inlineTestFile{data: []byte{0, 255, 1}, children: []inlineTestFile{raw("body")}}, "\x00\xff\x01body"},
		{"length from links", inlineTestFile{data: []byte("hello "), children: []inlineTestFile{raw("world")}, omitFileSize: true}, "hello world"},
		{"child size fallback", inlineTestFile{data: []byte("head:"), children: []inlineTestFile{{data: []byte("tail")}}, omitBlockSizes: true}, "head:tail"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ls := cidlink.DefaultLinkSystem()
			storage := cidlink.Memory{}
			ls.StorageReadOpener = storage.OpenRead
			ls.StorageWriteOpener = storage.OpenWrite
			root, _, _ := storeInlineTestFile(t, &ls, tc.file)
			decoded, err := ls.Load(ipld.LinkContext{}, root, dagpb.Type.PBNode)
			if err != nil {
				t.Fatal(err)
			}
			unixfsnode.AddUnixFSReificationToLinkSystem(&ls)
			for _, path := range []string{"file", "preload", "unixfs", "unixfs-preload"} {
				t.Run(path, func(t *testing.T) {
					ctx := context.Background()
					var f file.LargeBytesNode
					var err error
					switch path {
					case "file":
						f, err = file.NewUnixFSFile(ctx, decoded, &ls)
					case "preload":
						f, err = file.NewUnixFSFileWithPreload(ctx, decoded, &ls)
					default:
						var n ipld.Node
						n, err = ls.KnownReifiers[path](ipld.LinkContext{Ctx: ctx}, decoded, &ls)
						if err == nil {
							f = n.(file.LargeBytesNode)
						}
					}
					if err != nil {
						t.Fatal(err)
					}
					got, err := f.AsBytes()
					if err != nil {
						t.Fatal(err)
					}
					if string(got) != tc.want {
						t.Errorf("AsBytes: got %q, want %q", got, tc.want)
					}
					rs, err := f.AsLargeBytes()
					if err != nil {
						t.Fatal(err)
					}
					for _, whence := range []int{io.SeekStart, io.SeekCurrent, io.SeekEnd} {
						for offset := int64(0); offset <= int64(len(tc.want))+1; offset++ {
							relative := offset
							if whence == io.SeekCurrent {
								if _, err := rs.Seek(0, io.SeekStart); err != nil {
									t.Fatal(err)
								}
								if _, err := io.ReadFull(rs, make([]byte, 1)); err != nil {
									t.Fatal(err)
								}
								relative--
							} else if whence == io.SeekEnd {
								relative -= int64(len(tc.want))
							}
							pos, err := rs.Seek(relative, whence)
							if err != nil {
								t.Fatal(err)
							}
							if pos != offset {
								t.Errorf("Seek(%d, %d): got position %d, want %d", relative, whence, pos, offset)
							}
							got, err := io.ReadAll(rs)
							if err != nil {
								t.Fatal(err)
							}
							want := ""
							if offset < int64(len(tc.want)) {
								want = tc.want[offset:]
							}
							if string(got) != want {
								t.Errorf("Seek(%d, %d): got %q, want %q", relative, whence, got, want)
							}
						}
					}
				})
			}
		})
	}
}

func TestInlineDataBeforeLoadingLinks(t *testing.T) {
	ls := cidlink.DefaultLinkSystem()
	storage := cidlink.Memory{}
	ls.StorageReadOpener = storage.OpenRead
	ls.StorageWriteOpener = storage.OpenWrite
	root, _, _ := storeInlineTestFile(t, &ls, inlineTestFile{
		data:     []byte("prefix"),
		children: []inlineTestFile{{data: []byte("child"), raw: true}},
	})
	decoded, err := ls.Load(ipld.LinkContext{}, root, dagpb.Type.PBNode)
	if err != nil {
		t.Fatal(err)
	}
	unavailable := errors.New("child block unavailable")
	ls.StorageReadOpener = func(ipld.LinkContext, ipld.Link) (io.Reader, error) {
		return nil, unavailable
	}
	f, err := file.NewUnixFSFile(context.Background(), decoded, &ls)
	if err != nil {
		t.Fatal(err)
	}
	rs, err := f.AsLargeBytes()
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len("prefix"))
	if _, err := io.ReadFull(rs, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "prefix" {
		t.Errorf("inline bytes: got %q, want %q", buf, "prefix")
	}
	if n, err := rs.Read(make([]byte, 1)); n != 0 || !errors.Is(err, unavailable) {
		t.Errorf("child read: got (%d, %v), want (0, %v)", n, err, unavailable)
	}
	if _, err := file.NewUnixFSFileWithPreload(context.Background(), decoded, &ls); !errors.Is(err, unavailable) {
		t.Errorf("preload: got %v, want %v", err, unavailable)
	}
}
