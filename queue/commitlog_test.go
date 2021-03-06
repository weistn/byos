package queue

import (
	"os"
	"testing"
)

func TestCommit(t *testing.T) {
	os.Remove("test.log")
	c := newCommitLog()
	if err := c.create("test.log"); err != nil {
		t.Fatal(err)
	}

	var a appendAction
	a.a.flags = flagAppend
	a.a.streamName = "s1"
	a.a.offset = 0
	a.data = []byte("Hello World")
	if err := c.commit(&a); err != nil {
		t.Fatal(err)
	}

	a.a.streamName = "s1"
	a.a.offset = 11
	a.data = []byte("!Great!")
	if err := c.commit(&a); err != nil {
		t.Fatal(err)
	}

	a.a.flags = flagAppend
	a.a.streamName = "a1"
	a.a.offset = 0
	a.data = []byte("This is A")
	if err := c.commit(&a); err != nil {
		t.Fatal(err)
	}

	a.a.flags = flagAppend
	a.a.streamName = "b1"
	a.a.offset = 0
	a.data = []byte("This is B")
	if err := c.commit(&a); err != nil {
		t.Fatal(err)
	}

	span, err := c.streamRange("s1")
	if err != nil {
		t.Fatal(err)
	}
	if span.From != 0 || span.To != 18 {
		t.Fatal("streamRange")
	}

	var data2 [18]byte
	n, err := c.readStream("s1", 0, data2[:])
	if err != nil || len(data2) != n {
		t.Fatal(n, err)
	}
	if string(data2[:]) != "Hello World!Great!" {
		t.Fatal(string(data2[:]), "Wrong text")
	}

	var data3 [11]byte
	n, err = c.readStream("s1", 6, data3[:])
	if err != nil || len(data3) != n {
		t.Fatal(n, err)
	}
	if string(data3[:]) != "World!Great" {
		t.Fatal(string(data3[:]), "Wrong text")
	}

	var p pollardAction
	p.a.flags = flagPollard
	p.a.streamName = "s1"
	p.a.offset = span.To
	p.pollardPos = 6
	if err := c.commit(&p); err != nil {
		t.Fatal(err)
	}

	span, err = c.streamRange("s1")
	if err != nil {
		t.Fatal(err)
	}
	if span.From != 6 || span.To != 18 {
		t.Fatal("streamRange")
	}

	var data [12]byte
	n, err = c.readStream("s1", 6, data[:])
	if err != nil || len(data) != n {
		t.Fatal(n, err)
	}
	if string(data[:]) != "World!Great!" {
		t.Fatal(string(data[:]), "Wrong text")
	}

	if err = c.close(); err != nil {
		t.Fatal(err)
	}

	c2 := newCommitLog()
	if err = c2.recover("test.log"); err != nil {
		t.Fatal(err)
	}

	span, err = c2.streamRange("s1")
	if err != nil {
		t.Fatal(err)
	}
	if span.From != 6 || span.To != 18 {
		t.Fatal("streamRange")
	}

	n, err = c2.readStream("s1", 6, data[:])
	if err != nil || len(data) != n {
		t.Fatal(n, err)
	}
	if string(data[:]) != "World!Great!" {
		t.Fatal(string(data[:]), "Wrong text")
	}

	if err = c2.finalize(); err != nil {
		t.Fatal(err)
	}

	r := newLogReader("test.log")
	if err := r.open(); err != nil {
		t.Fatal(err)
	}
	e, err := r.search("s1")
	if err != nil {
		t.Fatal(err)
	}
	if e.span.From != 6 || e.span.To != 18 {
		t.Fatal("Wrong range")
	}
	var data4 [6]byte
	if err = r.read(e, 7, data4[:]); err != nil {
		t.Fatal(err)
	}
	if string(data4[:]) != "orld!G" {
		t.Fatal("Wrong data", string(data4[:]))
	}
}
