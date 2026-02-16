package testutil

import (
	"bytes"
	"crypto/rand"
	"io"
	"math/big"
	"sort"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data/builder"
	"github.com/ipfs/go-unixfsnode/testutil/namegen"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

type options struct {
	randReader     io.Reader
	shardBitwidth  int
	chunker        string
	dirname        string
	childGenerator ChildGeneratorFn
	shardThisDir   bool // a private option used internally to randomly switch on sharding at this current level
}

// Option is a functional option for the Generate* functions.
type Option func(*options)

// WithRandReader sets the random reader used by the Generate* functions.
func WithRandReader(randReader io.Reader) Option {
	return func(o *options) {
		o.randReader = randReader
	}
}

// WithShardBitwidth sets the shard fanout for the generated directory. By
// default directories are not sharded. Set to 8 to use the default sharding
// fanout value of 256. Set to a lower value, such as 4, to increase the
// probability of collisions and therefore greater depth for smaller number of
// files.
func WithShardBitwidth(bitwidth int) Option {
	return func(o *options) {
		o.shardBitwidth = bitwidth
	}
}

// WithChunker sets the chunker used by the Generate* functions. By default
// files are chunked using the "size-256144" chunker.
// The "size-256144" chunker will result in splitting bytes at 256144b
// boundaries. See https://pkg.go.dev/github.com/ipfs/go-ipfs-chunker#FromString
// for more information on options available.
func WithChunker(chunker string) Option {
	return func(o *options) {
		o.chunker = chunker
	}
}

// WithDirname sets the directory name used by UnixFSDirectory where a root
// directory name is required.
func WithDirname(dirname string) Option {
	return func(o *options) {
		o.dirname = dirname
	}
}

// ChildGeneratorFn is a function that generates a child DirEntry for a
// directory. It is used by UnixFSDirectory where control over the direct
// children of a directory is required. Return nil to stop generating children.
type ChildGeneratorFn func(name string) (*DirEntry, error)

// WithChildGenerator sets the child generator used by UnixFSDirectory control
// over the direct children of a directory is required.
func WithChildGenerator(childGenerator ChildGeneratorFn) Option {
	return func(o *options) {
		o.childGenerator = childGenerator
	}
}

// shardThisDir is a private internal option
func shardThisDir(b bool) Option {
	return func(o *options) {
		o.shardThisDir = b
	}
}

func applyOptions(opts []Option) *options {
	o := &options{
		randReader:    rand.Reader,
		shardBitwidth: 0,
		chunker:       "size-256144",
		shardThisDir:  true,
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// Generate a file of `size` random bytes, packaged into UnixFS structure,
// stored in the provided LinkSystem and returns a DirEntry representation of
// the file.
func UnixFSFile(lsys linking.LinkSystem, size int, opt ...Option) (DirEntry, error) {
	o := applyOptions(opt)
	delimited := io.LimitReader(o.randReader, int64(size))
	var buf bytes.Buffer
	buf.Grow(size)
	delimited = io.TeeReader(delimited, &buf)
	cids := make([]cid.Cid, 0)
	var undo func()
	lsys.StorageWriteOpener, undo = cidCollector(lsys, &cids)
	defer undo()
	root, gotSize, err := builder.BuildUnixFSFile(delimited, o.chunker, &lsys)
	if err != nil {
		return DirEntry{}, err
	}
	return DirEntry{
		Path:     "",
		Content:  buf.Bytes(),
		Root:     root.(cidlink.Link).Cid,
		SelfCids: cids,
		TSize:    uint64(gotSize),
	}, nil
}

// UnixFSDirectory generates a random UnixFS directory that aims for the
// requested targetSize (in bytes, although it is likely to fall somewhere
// under this number), storing the blocks in the provided LinkSystem and
// returns a DirEntry representation of the directory.
//
// If the WithDirname option is not set, the directory will be built as a root
// directory. If the dirname option is set, the directory will be built as a
// child directory.
//
// If the WithChildGenerator option is not set, the targetSize will be
// ignored and all sizing control will be delegated to the child generator.
func UnixFSDirectory(lsys linking.LinkSystem, targetSize int, opts ...Option) (DirEntry, error) {
	o := applyOptions(opts)

	var curSize int
	targetFileSize := targetSize / 16

	children := make([]DirEntry, 0)

	childGenerator := func(name string) (*DirEntry, error) {
		for curSize < targetSize {
			switch rndInt(o.randReader, 6) {
			case 0: // 1 in 6 chance of finishing this directory if not at root
				if o.dirname != "" && len(children) > 0 {
					curSize = targetSize // not really, but we're done with this directory
				} // else at the root we don't get to finish early
			case 1: // 1 in 6 chance of making a new directory
				if targetSize-curSize <= 1024 { // don't make tiny directories
					continue
				}
				so := append(opts, WithDirname(o.dirname+"/"+name), shardThisDir(rndInt(o.randReader, 6) == 0))
				child, err := UnixFSDirectory(lsys, targetSize-curSize, so...)
				if err != nil {
					return nil, err
				}
				children = append(children, child)
				curSize += int(child.TSize)
			default: // 4 in 6 chance of making a new file
				var size int
				for size == 0 { // don't make empty files
					sizeB, err := rand.Int(o.randReader, big.NewInt(int64(targetFileSize)))
					if err != nil {
						return nil, err
					}
					size = min(int(sizeB.Int64()), targetSize-curSize)
				}
				entry, err := UnixFSFile(lsys, size, opts...)
				if err != nil {
					return nil, err
				}
				var name string
				entry.Path = o.dirname + "/" + name
				curSize += size
				return &entry, nil
			}
		}
		return nil, nil
	}

	if o.childGenerator != nil {
		childGenerator = o.childGenerator
	}

	for {
		var name string
		for {
			var err error
			name, err = namegen.RandomDirectoryName(o.randReader)
			if err != nil {
				return DirEntry{}, err
			}
			if !isDupe(children, name) {
				break
			}
		}

		child, err := childGenerator(o.dirname + "/" + name)
		if err != nil {
			return DirEntry{}, err
		}
		if child == nil {
			break
		}
		children = append(children, *child)
	}

	var shardBitwidth int
	if o.shardThisDir {
		shardBitwidth = o.shardBitwidth
	}
	dirEntry, err := packDirectory(lsys, children, shardBitwidth)
	if err != nil {
		return DirEntry{}, err
	}
	dirEntry.Path = o.dirname
	return dirEntry, nil
}

// GenerateFile generates a random unixfs file of the given size, storing the
// blocks in the provided LinkSystem and returns a DirEntry representation of
// the file.
//
// This function will be deprecated in a future release, use UnixFSFile()
// instead.
func GenerateFile(t require.TestingT, linkSys *linking.LinkSystem, randReader io.Reader, size int) DirEntry {
	dirEntry, err := UnixFSFile(*linkSys, size, WithRandReader(randReader))
	require.NoError(t, err)
	return dirEntry
}

// GenerateDirectory generates a random UnixFS directory that aims for the
// requested targetSize (in bytes, although it is likely to fall somewhere
// under this number), storing the blocks in the provided LinkSystem and
// returns a DirEntry representation of the directory. If rootSharded is true,
// the root directory will be built as HAMT sharded (with a low "width" to
// maximise the chance of collisions and therefore greater depth for smaller
// number of files).
//
// This function will be deprecated in a future release, use UnixFSDirectory()
// instead.
func GenerateDirectory(t require.TestingT, linkSys *linking.LinkSystem, randReader io.Reader, targetSize int, rootSharded bool) DirEntry {
	return GenerateDirectoryFrom(t, linkSys, randReader, targetSize, "", rootSharded)
}

// GenerateDirectoryFrom is the same as GenerateDirectory but allows the caller
// to specify a directory path to start from. This is useful for generating
// nested directories.
//
// This function will be deprecated in a future release, use UnixFSDirectory()
// instead.
func GenerateDirectoryFrom(
	t require.TestingT,
	linkSys *linking.LinkSystem,
	randReader io.Reader,
	targetSize int,
	dir string,
	sharded bool,
) DirEntry {
	var curSize int
	targetFileSize := targetSize / 16
	children := make([]DirEntry, 0)
	for curSize < targetSize {
		switch rndInt(randReader, 6) {
		case 0: // 1 in 6 chance of finishing this directory if not at root
			if dir != "" && len(children) > 0 {
				curSize = targetSize // not really, but we're done with this directory
			} // else at the root we don't get to finish early
		case 1: // 1 in 6 chance of making a new directory
			if targetSize-curSize <= 1024 { // don't make tiny directories
				continue
			}
			var newDir string
			for {
				var err error
				newDir, err = namegen.RandomDirectoryName(randReader)
				require.NoError(t, err)
				if !isDupe(children, newDir) {
					break
				}
			}
			sharded := rndInt(randReader, 6) == 0
			child := GenerateDirectoryFrom(t, linkSys, randReader, targetSize-curSize, dir+"/"+newDir, sharded)
			children = append(children, child)
			curSize += int(child.TSize)
		default: // 4 in 6 chance of making a new file
			var size int
			for size == 0 { // don't make empty files
				sizeB, err := rand.Int(randReader, big.NewInt(int64(targetFileSize)))
				require.NoError(t, err)
				size = min(int(sizeB.Int64()), targetSize-curSize)
			}
			entry := GenerateFile(t, linkSys, randReader, size)
			var name string
			for {
				var err error
				name, err = namegen.RandomFileName(randReader)
				require.NoError(t, err)
				if !isDupe(children, name) {
					break
				}
			}
			entry.Path = dir + "/" + name
			curSize += size
			children = append(children, entry)
		}
	}
	dirEntry := BuildDirectory(t, linkSys, children, sharded)
	dirEntry.Path = dir
	return dirEntry
}

// BuildDirectory builds a directory from the given children, storing the
// blocks in the provided LinkSystem and returns a DirEntry representation of
// the directory. If sharded is true, the root directory will be built as HAMT
// sharded (with a low "width" to maximise the chance of collisions and
// therefore greater depth for smaller number of files).
//
// This function will be deprecated in a future release. Currently there is no
// direct replacement.
func BuildDirectory(t require.TestingT, linkSys *linking.LinkSystem, children []DirEntry, sharded bool) DirEntry {
	var bitWidth int
	if sharded {
		// fanout of 16, quite small to increase collision probability so we actually get sharding
		bitWidth = 4
	}
	dirEnt, err := packDirectory(*linkSys, children, bitWidth)
	require.NoError(t, err)
	return dirEnt
}

func packDirectory(lsys linking.LinkSystem, children []DirEntry, bitWidth int) (DirEntry, error) {
	// create stable sorted children, which should match the encoded form
	// in dag-pb
	sort.Slice(children, func(i, j int) bool {
		return strings.Compare(children[i].Path, children[j].Path) < 0
	})

	dirLinks := make([]dagpb.PBLink, 0)
	for _, child := range children {
		paths := strings.Split(child.Path, "/")
		name := paths[len(paths)-1]
		lnk, err := builder.BuildUnixFSDirectoryEntry(name, int64(child.TSize), cidlink.Link{Cid: child.Root})
		if err != nil {
			return DirEntry{}, err
		}
		dirLinks = append(dirLinks, lnk)
	}
	cids := make([]cid.Cid, 0)
	var undo func()
	lsys.StorageWriteOpener, undo = cidCollector(lsys, &cids)
	defer undo()
	var root ipld.Link
	var size uint64
	var err error
	if bitWidth > 0 {
		// width is 2^bitWidth
		width := 2 << bitWidth
		const hasher = multihash.MURMUR3X64_64
		root, size, err = builder.BuildUnixFSShardedDirectory(width, hasher, dirLinks, &lsys)
		if err != nil {
			return DirEntry{}, err
		}
	} else {
		root, size, err = builder.BuildUnixFSDirectory(dirLinks, &lsys)
		if err != nil {
			return DirEntry{}, err
		}
	}

	return DirEntry{
		Path:     "",
		Root:     root.(cidlink.Link).Cid,
		SelfCids: cids,
		TSize:    size,
		Children: children,
	}, nil
}

func rndInt(randReader io.Reader, max int) int {
	coin, err := rand.Int(randReader, big.NewInt(int64(max)))
	if err != nil {
		return 0 // eh, whatever
	}
	return int(coin.Int64())
}

func cidCollector(ls ipld.LinkSystem, cids *[]cid.Cid) (ipld.BlockWriteOpener, func()) {
	swo := ls.StorageWriteOpener
	return func(linkCtx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
			w, c, err := swo(linkCtx)
			if err != nil {
				return nil, nil, err
			}
			return w, func(lnk ipld.Link) error {
				*cids = append(*cids, lnk.(cidlink.Link).Cid)
				return c(lnk)
			}, nil
		}, func() {
			// reset
			ls.StorageWriteOpener = swo
		}
}

func isDupe(children []DirEntry, name string) bool {
	if strings.Contains(name, ".") {
		name = name[:strings.LastIndex(name, ".")]
	}
	for _, child := range children {
		childName := child.Path[strings.LastIndex(child.Path, "/")+1:]
		// remove suffix
		if strings.Contains(childName, ".") {
			childName = childName[:strings.LastIndex(childName, ".")]
		}
		if childName == name {
			return true
		}
	}
	return false
}
