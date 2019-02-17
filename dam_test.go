// dam_test.go
//
// To the extent possible under law, Ivan Markin waived all copyright
// and related or neighboring rights to this module of dam, using the creative
// commons "CC0" public domain dedication. See LICENSE or
// <http://creativecommons.org/publicdomain/zero/1.0/> for full details.

package dam

import (
	"errors"
	"testing"
	"time"

	"github.com/matryer/is"
)

var MockErr = errors.New("mock error")

type TestStruct struct {
	Value string
}

func (ts *TestStruct) Marshal() ([]byte, error) {
	if ts.Value == "" {
		return nil, MockErr
	}
	return []byte(ts.Value), nil
}

func TestNewNoPurge(t *testing.T) {
	is := is.New(t)
	d := New(NoPurge)
	is.True(d.storage != nil)
	is.True(d.ticker == nil)
}

func TestNew(t *testing.T) {
	is := is.New(t)
	d := New(1 * time.Second)
	defer d.Stop()
	is.True(d.storage != nil)
	is.True(d.ticker != nil)
}

func TestStop(t *testing.T) {
	is := is.New(t)
	d := New(1 * time.Millisecond)
	d.Stop()
	key := &TestStruct{Value: "key"}
	d.Store(key, "value")
	time.Sleep(200 * time.Millisecond)
	is.True(len(d.storage) == 1)
}

func TestStoreMarshalErr(t *testing.T) {
	is := is.New(t)
	d := New(NoPurge)
	key := &TestStruct{}
	err := d.Store(key, "testing")
	is.True(err == MockErr)
}

func TestStoreNoPurge(t *testing.T) {
	is := is.New(t)
	d := New(NoPurge)
	key := &TestStruct{Value: "key"}
	err := d.Store(key, "value")
	is.NoErr(err)
	is.True(len(d.storage) == 1)
}

func TestStore(t *testing.T) {
	is := is.New(t)
	d := New(100 * time.Millisecond)
	defer d.Stop()
	key := &TestStruct{Value: "key"}
	err := d.Store(key, "value")
	is.NoErr(err)
	is.Equal(len(d.storage), 1)
	time.Sleep(150 * time.Millisecond)
	is.Equal(len(d.storage), 0)
}

func TestLoadMarshalErr(t *testing.T) {
	is := is.New(t)
	d := New(100 * time.Millisecond)
	defer d.Stop()
	key := &TestStruct{}
	v, err := d.Load(key)
	is.True(err == MockErr)
	is.Equal(v, nil)
}

func TestLoad(t *testing.T) {
	is := is.New(t)
	d := New(100 * time.Millisecond)
	key := &TestStruct{Value: "key"}
	err := d.Store(key, "value")
	is.NoErr(err)
	v, err := d.Load(key)
	is.NoErr(err)
	is.True(v != nil)
	val, ok := v.(string)
	is.True(ok)
	is.Equal(val, "value")
}

func TestDelete(t *testing.T) {
	is := is.New(t)
	d := New(NoPurge)
	key := &TestStruct{Value: "key"}
	err := d.Store(key, "value")
	is.NoErr(err)
	err = d.Delete(key)
	is.NoErr(err)
	v, err := d.Load(key)
	is.Equal(err, ErrNotFound)
	is.Equal(v, nil)
}

func TestRangeSimple(t *testing.T) {
	is := is.New(t)
	d := New(NoPurge)
	key1 := &TestStruct{Value: "key"}
	err := d.Store(key1, "value1")
	is.NoErr(err)
	key2 := &TestStruct{Value: "key2"}
	err = d.Store(key2, "value2")
	is.NoErr(err)
	rr := []string{"value1", "value2"}
	r := map[string]bool{}
	d.Range(func(v interface{}) bool {
		is.True(v != nil)
		val, ok := v.(string)
		is.True(ok)
		r[val] = true
		return true
	})
	for _, v := range rr {
		ok, ok2 := r[v]
		is.True(ok)
		is.True(ok2)
	}
}

func TestLoadOrStore(t *testing.T) {
	is := is.New(t)
	fetch := func() (interface{}, error) {
		return "value2", nil
	}
	d := New(100 * time.Millisecond)
	key := &TestStruct{Value: "key"}
	err := d.Store(key, "value")
	is.NoErr(err)
	v, err := d.LoadOrStore(key, fetch)
	is.NoErr(err)
	is.True(v != nil)
	val, ok := v.(string)
	is.True(ok)
	is.Equal(val, "value")

	time.Sleep(130 * time.Millisecond)
	v, err = d.LoadOrStore(key, fetch)
	is.NoErr(err)
	is.True(v != nil)
	val, ok = v.(string)
	is.True(ok)
	is.Equal(val, "value2")
}

func TestSize(t *testing.T) {
	is := is.New(t)
	d := New(NoPurge)
	for i := 0; i < 128; i++ {
		err := d.Store(Key(i), i)
		is.NoErr(err)
	}
	is.Equal(d.Size(), 128)
	d.Purge()
	is.Equal(d.Size(), 0)
}

func TestKey(t *testing.T) {
	is := is.New(t)
	x := struct {
		A int
		B string
		C []byte
	}{
		1,
		"b",
		[]byte("c"),
	}
	mx := Key(x)
	bx, err := hash(mx)
	is.NoErr(err)
	is.Equal(bx, "\x13\x83\x87\xb5\xe1\x7a\xf3\xe3")

	mx2 := Key(mx)
	is.Equal(mx2, mx)
}

func TestLockUnlock(t *testing.T) {
	is := is.New(t)
	d := New(NoPurge)
	err := d.Store(Key("test1"), "test1")
	is.NoErr(err)
	err = d.Store(Key("test2"), "test2")
	is.NoErr(err)
	n := 0
	d.Lock()
	go func() {
		err := d.Store(Key("test3"), "test3")
		is.NoErr(err)
	}()
	time.Sleep(10 * time.Millisecond)
	d.Range(func(v interface{}) bool {
		n++
		return true
	})
	d.Unlock()
	is.Equal(n, 2)
}

func TestLockUnlockDeletePurge(t *testing.T) {
	is := is.New(t)
	d := New(NoPurge)
	err := d.Store(Key("test1"), "test1")
	is.NoErr(err)
	err = d.Store(Key("test2"), "test2")
	is.NoErr(err)
	n := 0
	d.Lock()
	go d.Purge()
	go func() {
		err := d.Delete(Key("test2"))
		is.NoErr(err)
	}()
	time.Sleep(10 * time.Millisecond)
	d.Range(func(v interface{}) bool {
		n++
		return true
	})
	d.Unlock()
	is.Equal(n, 2)
}
