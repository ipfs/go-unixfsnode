package namegen_test

import (
	crand "crypto/rand"
	"math/rand/v2"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/ipfs/go-unixfsnode/testutil/namegen"
	"github.com/stretchr/testify/require"
)

func TestRandomName(t *testing.T) {
	var seed [32]byte
	_, _ = crand.Read(seed[:])
	rndSrc := rand.NewChaCha8(seed)

	name, err := namegen.RandomName(rndSrc, 256, 256)
	require.NoError(t, err)
	require.Equal(t, 256, utf8.RuneCountInString(name))

	// Check that each character in name is from the namegen.Alphabet.
	for _, ch := range name {
		require.Containsf(t, namegen.Alphabet, ch, "namegen.Alphabet does not contain %q", string(ch))
	}

	// Check that all possible characters are generated.
	wantChars := make(map[rune]struct{}, len(namegen.Alphabet))
	for _, ch := range namegen.Alphabet {
		wantChars[ch] = struct{}{}
	}
	const maxTries = 10000
	var tries int
	for len(wantChars) != 0 {
		if tries == maxTries {
			var b strings.Builder
			for ch := range wantChars {
				b.WriteRune(ch)
			}
			t.Fatal("did not generate all posible characters, missing", b.String())
		}
		name, err = namegen.RandomName(rndSrc, 256, 256)
		require.NoError(t, err)
		for _, ch := range name {
			delete(wantChars, ch)
		}
		tries++

	}
}

func TestRandomFileName(t *testing.T) {
	var seed [32]byte
	_, _ = crand.Read(seed[:])
	rndSrc := rand.NewChaCha8(seed)

	for range 5 {
		name, err := namegen.RandomFileName(rndSrc)
		require.NoError(t, err)
		t.Log(name)
	}
}
