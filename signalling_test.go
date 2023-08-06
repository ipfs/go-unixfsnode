package unixfsnode_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/stretchr/testify/require"
)

// Selectors are tested against JSON expected forms; this doesn't necessarily
// validate that they work as advertised.	It's just a sanity check that the
// selectors are being built as expected.

var exploreAllJson = mustDagJson(selectorparse.CommonSelector_ExploreAllRecursively)

// explore interpret-as (~), next (>), match (.), interpreted as unixfs-preload
var matchUnixfsPreloadJson = `{"~":{">":{".":{}},"as":"unixfs-preload"}}`

// explore interpret-as (~), next (>), union (|) of match (.) and explore recursive (R) edge (@) with a depth of 1, interpreted as unixfs
var matchUnixfsEntityJson = `{"~":{">":{"|":[{".":{}},{"R":{":>":{"a":{">":{"@":{}}}},"l":{"depth":1}}}]},"as":"unixfs"}}`

// match interpret-as (~), next (>), match (.), interpreted as unixfs
var matchUnixfsJson = `{"~":{">":{".":{}},"as":"unixfs"}}`

func TestUnixFSPathSelector(t *testing.T) {
	testCases := []struct {
		name             string
		path             string
		expextedSelector string
	}{
		{
			name:             "empty path",
			path:             "",
			expextedSelector: matchUnixfsJson,
		},
		{
			name:             "single field",
			path:             "/foo",
			expextedSelector: jsonFields(matchUnixfsJson, "foo"),
		},
		{
			name:             "multiple fields",
			path:             "/foo/bar",
			expextedSelector: jsonFields(matchUnixfsJson, "foo", "bar"),
		},
		{
			name:             "leading slash optional",
			path:             "foo/bar",
			expextedSelector: jsonFields(matchUnixfsJson, "foo", "bar"),
		},
		{
			name:             "trailing slash optional",
			path:             "/foo/bar/",
			expextedSelector: jsonFields(matchUnixfsJson, "foo", "bar"),
		},
		{
			// a go-ipld-prime specific thing, not clearly specified by path spec (?)
			name:             ".. is a field named ..",
			path:             "/foo/../bar/",
			expextedSelector: jsonFields(matchUnixfsJson, "foo", "..", "bar"),
		},
		{
			// a go-ipld-prime specific thing, not clearly specified by path spec
			name:             "redundant slashes ignored",
			path:             "foo///bar",
			expextedSelector: jsonFields(matchUnixfsJson, "foo", "bar"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sel := unixfsnode.UnixFSPathSelector(tc.path)
			require.Equal(t, tc.expextedSelector, mustDagJson(sel))
		})
	}
}

func TestUnixFSPathSelectorBuilder(t *testing.T) {
	testCases := []struct {
		name             string
		path             string
		target           builder.SelectorSpec
		matchPath        bool
		expextedSelector string
	}{
		{
			name:             "empty path",
			path:             "",
			target:           unixfsnode.ExploreAllRecursivelySelector,
			expextedSelector: exploreAllJson,
		},
		{
			name:             "empty path shallow (preload)",
			path:             "",
			target:           unixfsnode.MatchUnixFSPreloadSelector,
			expextedSelector: matchUnixfsPreloadJson,
		},
		{
			name:             "empty path shallow (entity)",
			path:             "",
			target:           unixfsnode.MatchUnixFSEntitySelector,
			expextedSelector: matchUnixfsEntityJson,
		},
		{
			name:             "single field",
			path:             "/foo",
			expextedSelector: jsonFields(exploreAllJson, "foo"),
			target:           unixfsnode.ExploreAllRecursivelySelector,
		},
		{
			name:             "single field, match path",
			path:             "/foo",
			expextedSelector: jsonFieldsMatchPoint(exploreAllJson, "foo"),
			target:           unixfsnode.ExploreAllRecursivelySelector,
			matchPath:        true,
		},
		{
			name:             "single field shallow (preload)",
			path:             "/foo",
			expextedSelector: jsonFields(matchUnixfsPreloadJson, "foo"),
			target:           unixfsnode.MatchUnixFSPreloadSelector,
		},
		{
			name:             "single field shallow (entity)",
			path:             "/foo",
			expextedSelector: jsonFields(matchUnixfsEntityJson, "foo"),
			target:           unixfsnode.MatchUnixFSEntitySelector,
		},
		{
			name:             "multiple fields",
			path:             "/foo/bar",
			expextedSelector: jsonFields(exploreAllJson, "foo", "bar"),
			target:           unixfsnode.ExploreAllRecursivelySelector,
		},
		{
			name:             "multiple fields, match path",
			path:             "/foo/bar",
			expextedSelector: jsonFieldsMatchPoint(exploreAllJson, "foo", "bar"),
			target:           unixfsnode.ExploreAllRecursivelySelector,
			matchPath:        true,
		},
		{
			name:             "multiple fields shallow",
			path:             "/foo/bar",
			expextedSelector: jsonFields(matchUnixfsPreloadJson, "foo", "bar"),
			target:           unixfsnode.MatchUnixFSPreloadSelector,
		},
		{
			name:             "leading slash optional",
			path:             "foo/bar",
			expextedSelector: jsonFields(exploreAllJson, "foo", "bar"),
			target:           unixfsnode.ExploreAllRecursivelySelector,
		},
		{
			name:             "trailing slash optional",
			path:             "/foo/bar/",
			expextedSelector: jsonFields(exploreAllJson, "foo", "bar"),
			target:           unixfsnode.ExploreAllRecursivelySelector,
		},
		// a go-ipld-prime specific thing, not clearly specified by path spec (?)
		{
			name:             ".. is a field named ..",
			path:             "/foo/../bar/",
			expextedSelector: jsonFields(exploreAllJson, "foo", "..", "bar"),
			target:           unixfsnode.ExploreAllRecursivelySelector,
		},
		{
			// a go-ipld-prime specific thing, not clearly specified by path spec
			name:             "redundant slashes ignored",
			path:             "foo///bar",
			expextedSelector: jsonFields(exploreAllJson, "foo", "bar"),
			target:           unixfsnode.ExploreAllRecursivelySelector,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sel := unixfsnode.UnixFSPathSelectorBuilder(tc.path, tc.target, tc.matchPath)
			require.Equal(t, tc.expextedSelector, mustDagJson(sel))
		})
	}
}

func jsonFields(target string, fields ...string) string {
	var sb strings.Builder
	for _, n := range fields {
		// explore interpret-as (~) next (>), explore field (f) + specific field (f>), with field name
		sb.WriteString(fmt.Sprintf(`{"~":{">":{"f":{"f>":{"%s":`, n))
	}
	sb.WriteString(target)
	sb.WriteString(strings.Repeat(`}}},"as":"unixfs"}}`, len(fields)))
	return sb.String()
}

func jsonFieldsMatchPoint(target string, fields ...string) string {
	var sb strings.Builder
	for _, n := range fields {
		// union (|) of match (.) and explore interpret-as (~) next (>), explore field (f) + specific field (f>), with field name
		sb.WriteString(fmt.Sprintf(`{"|":[{".":{}},{"~":{">":{"f":{"f>":{"%s":`, n))
	}
	sb.WriteString(target)
	sb.WriteString(strings.Repeat(`}}},"as":"unixfs"}}]}`, len(fields)))
	return sb.String()
}

func mustDagJson(n ipld.Node) string {
	byts, err := ipld.Encode(n, dagjson.Encode)
	if err != nil {
		panic(err)
	}
	return string(byts)
}
