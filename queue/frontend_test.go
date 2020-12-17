package queue

import (
	"os"
	"testing"
)

func TestFrontend(t *testing.T) {
	os.Remove("logs/commit_0000.log")
	f, err := NewFrontend("logs")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Append("ps1", []byte("Hello World!"), true); err != nil {
		t.Fatal(err)
	}
	if err := f.Append("ps1", []byte("Welcome to streams."), true); err != nil {
		t.Fatal(err)
	}
	stat, err := f.Stat("ps1")
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size != 31 {
		t.Fatal(stat.Size, err)
	}
	var buffer [7]byte
	n, err := f.Read("ps1", 0, buffer[:5])
	if err != nil || n != 5 || string(buffer[:5]) != "Hello" {
		t.Fatal(n, string(buffer[:5]), err)
	}
	n, err = f.Read("ps1", 6, buffer[:])
	if err != nil || n != 7 || string(buffer[:]) != "World!W" {
		t.Fatal(n, string(buffer[:]), err)
	}
	n, err = f.Read("ps1", 28, buffer[:])
	if err != nil || n != 3 || string(buffer[:3]) != "ms." {
		t.Fatal(n, string(buffer[:3]), err)
	}
	f.Close()
}
