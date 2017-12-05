// dam.go - simple periodically flushable cache
//
// To the extent possible under law, Ivan Markin waived all copyright
// and related or neighboring rights to this module of dam, using the creative
// commons "CC0" public domain dedication. See LICENSE or
// <http://creativecommons.org/publicdomain/zero/1.0/> for full details.

package dam

import (
	"errors"
	"sync"
	"time"

	"golang.org/x/crypto/blake2b"
)

var (
	ErrNotFound = errors.New("not found")
)

const (
	NoPurge = time.Duration(0)
)

type Marshallable interface {
	Marshal() ([]byte, error)
}

type Dam struct {
	mutex   sync.RWMutex
	storage map[string]interface{}

	ticker     *time.Ticker
	tickerDone chan struct{}
}

func New(duration time.Duration) *Dam {
	d := &Dam{
		storage:    make(map[string]interface{}),
		tickerDone: make(chan struct{}),
	}
	if duration > time.Duration(0) {
		d.ticker = time.NewTicker(duration)
		go func() {
			for {
				select {
				case <-d.ticker.C:
					d.Purge()
				case <-d.tickerDone:
					return
				}
			}
		}()
	}
	return d
}

func hash(s Marshallable) (string, error) {
	m, err := s.Marshal()
	if err != nil {
		return "", err
	}
	h, err := blake2b.New256(nil)
	if err != nil {
		panic(err)
	}
	h.Write(m)
	ret := string(m[:8])
	return ret, nil
}

func (d *Dam) Store(key Marshallable, value interface{}) error {
	k, err := hash(key)
	if err != nil {
		return err
	}
	d.mutex.Lock()
	d.storage[k] = value
	d.mutex.Unlock()
	return nil
}

func (d *Dam) Load(key Marshallable) (interface{}, error) {
	k, err := hash(key)
	if err != nil {
		return nil, err
	}
	d.mutex.RLock()
	value, ok := d.storage[k]
	d.mutex.RUnlock()
	if !ok {
		return nil, ErrNotFound
	}
	return value, nil
}

type FetchFunc func() (interface{}, error)

func (d *Dam) LoadOrStore(key Marshallable, fetch FetchFunc) (interface{}, error) {
	v, err := d.Load(key)
	if err == ErrNotFound {
		v, err = fetch()
		if err != nil {
			return nil, err
		}
		err = d.Store(key, v)
	}
	return v, err
}

func (d *Dam) Purge() {
	d.mutex.Lock()
	d.storage = make(map[string]interface{})
	d.mutex.Unlock()
}

func (d *Dam) Stop() {
	if d.ticker == nil {
		return
	}
	d.ticker.Stop()
	close(d.tickerDone)
}
