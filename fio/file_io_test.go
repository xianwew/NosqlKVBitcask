package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func prepareFileIO(t *testing.T, filename string) (*FileIO, string) {
	t.Helper()
	path := filepath.Join(os.TempDir(), filename)
	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	return fio, path
}

func cleanupFileIO(fio *FileIO, path string) {
	_ = fio.Close()
	_ = os.Remove(path)
}

func TestNewFileIOManager(t *testing.T) {
	fio, path := prepareFileIO(t, "a.data")
	defer cleanupFileIO(fio, path)
}

func TestFileIO_Write(t *testing.T) {
	fio, path := prepareFileIO(t, "a-write.data")
	defer cleanupFileIO(fio, path)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("bitcask kv"))
	assert.Equal(t, 10, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("storage"))
	assert.Equal(t, 7, n)
	assert.Nil(t, err)
}

func TestFileIO_Read(t *testing.T) {
	fio, path := prepareFileIO(t, "a-read.data")
	defer cleanupFileIO(fio, path)

	_, err := fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)

	b1 := make([]byte, 5)
	n, err := fio.Read(b1, 0)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), b1)

	b2 := make([]byte, 5)
	n, err = fio.Read(b2, 5)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-b"), b2)
}

func TestFileIO_Sync(t *testing.T) {
	fio, path := prepareFileIO(t, "a-sync.data")
	defer cleanupFileIO(fio, path)

	err := fio.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	fio, path := prepareFileIO(t, "a-close.data")
	defer cleanupFileIO(fio, path)

	err := fio.Close()
	assert.Nil(t, err)
}
