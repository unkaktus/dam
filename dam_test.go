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
	d := New(-1 * time.Second)
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
	d := New(-1 * time.Second)
	key := &TestStruct{}
	err := d.Store(key, "testing")
	is.True(err == MockErr)
}

func TestStoreNoPurge(t *testing.T) {
	is := is.New(t)
	d := New(-1 * time.Second)
	key := &TestStruct{Value: "key"}
	err := d.Store(key, "value")
	is.NoErr(err)
	is.True(len(d.storage) == 1)
}

func TestStore(t *testing.T) {
	is := is.New(t)
	d := New(10 * time.Millisecond)
	defer d.Stop()
	key := &TestStruct{Value: "key"}
	err := d.Store(key, "value")
	is.NoErr(err)
	is.Equal(len(d.storage), 1)
	time.Sleep(11 * time.Millisecond)
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
func TestLoadOrStore(t *testing.T) {
	is := is.New(t)
	fetch := func() (interface{}, error) {
		return "value2", nil
	}
	d := New(10 * time.Millisecond)
	key := &TestStruct{Value: "key"}
	err := d.Store(key, "value")
	is.NoErr(err)
	v, err := d.LoadOrStore(key, fetch)
	is.NoErr(err)
	is.True(v != nil)
	val, ok := v.(string)
	is.True(ok)
	is.Equal(val, "value")

	time.Sleep(12 * time.Millisecond)
	v, err = d.LoadOrStore(key, fetch)
	is.NoErr(err)
	is.True(v != nil)
	val, ok = v.(string)
	is.True(ok)
	is.Equal(val, "value2")
}
