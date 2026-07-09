package namegen

import (
	"encoding/binary"
	"io"
	"strings"
)

var Alphabet = []rune("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzáäåèêëíîïøúýþāăąċčďđĕęěğħķĺłňōŕřŝşšţťŧŭŵžƒƿǩνοςυвгкмпъяѐѓљғԁḽḿṕẓẽịỏụỳ")
var extensions = []string{"", ".bz2", ".car", ".csv", ".docx", ".jpg", ".json", ".md", ".mp4", ".pdf", ".png", ".tar", ".txt", ".txt.gz", ".xml", ".xz"}

func RandomName(r io.Reader, minSize, maxSize int) (string, error) {
	if minSize > maxSize {
		minSize, maxSize = maxSize, minSize
	}
	sizeDiff := maxSize - minSize
	size := minSize
	if sizeDiff != 0 {
		rn, err := randIntn(r, sizeDiff+1)
		if err != nil {
			return "", err
		}
		size += rn
	}

	if size <= 0 {
		return "", nil
	}
	src := make([]byte, size)
	r.Read(src)
	var b strings.Builder
	for i := range src {
		ch := Alphabet[int(src[i])%len(Alphabet)]
		b.WriteRune(ch)
	}
	return b.String(), nil
}

// RandomDirectoryName returns a random directory name from the provided word list.
func RandomDirectoryName(r io.Reader) (string, error) {
	return RandomName(r, 5, 20)
}

// RandomFileName returns a random file name with an extension from the provided word list and common extensions.
func RandomFileName(r io.Reader) (string, error) {
	name, err := RandomName(r, 5, 20)
	if err != nil {
		return "", err
	}
	ext, err := RandomFileExtension(r)
	if err != nil {
		return "", err
	}
	return name + ext, nil
}

// RandomFileExtension returns a random file extension, including '.'. This may
// also return an empty string.
func RandomFileExtension(r io.Reader) (string, error) {
	index, err := randIntn(r, len(extensions))
	if err != nil {
		return "", err
	}
	return extensions[index], nil
}

func randIntn(r io.Reader, max int) (int, error) {
	var buf32 [4]byte
	_, err := io.ReadFull(r, buf32[:])
	if err != nil {
		return 0, err
	}
	n := binary.BigEndian.Uint32(buf32[:])
	return int(n % uint32(max)), nil
}
