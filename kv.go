package kv

import (
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"sync"
)

// ByKey implements the sort interface for a []*KeyValue
// e.g., sort.Sort(ByKey(some slice of pointers to KeyValues))
type ByKey []*KeyValue

func (kv ByKey) Len() int           { return len(kv) }
func (kv ByKey) Swap(i, j int)      { kv[i], kv[j] = kv[j], kv[i] }
func (kv ByKey) Less(i, j int) bool { return bytes.Compare(kv[i].Key, kv[j].Key) < 0 }

// KeyValue stores a key and a value pair, both of which are a slice of bytes
// Use sort.Sort(ByKey([]*KeyValue)) to sort them lexographically
type KeyValue struct {
	Key   []byte `json:"k"`
	Value []byte `json:"v"`
}

// BulkSorter sorts many KeyValues using temp files
type bulkSorter struct {
	m              *sync.RWMutex
	batchSize      int
	tmpPaths       []string
	entityLocation map[string]int
	buffer         []*KeyValue
	i              int
}

func newBulkSorter(batchSize int) *bulkSorter {
	return &bulkSorter{
		m:              new(sync.RWMutex),
		batchSize:      batchSize,
		entityLocation: make(map[string]int),
		buffer:         make([]*KeyValue, batchSize),
		i:              0,
	}
}

// BulkSort sorts a large stream of KeyValue pairs using intermediate temp files
func BulkSort(inCh chan *KeyValue, batchSize int) (outCh chan *KeyValue) {
	outCh = make(chan *KeyValue)
	s := newBulkSorter(batchSize)
	go func() {
		for x := range inCh {
			s.buffer[s.i] = x
			s.i++
			if s.i == s.batchSize {
				s.flushBuffer()
			}
		}
		if s.i > 0 {
			s.flushBuffer()
		}
		for x := range s.sortedRecords() {
			outCh <- x
		}
		s.cleanup()
		close(outCh)
	}()
	return outCh
}

func (s *bulkSorter) cleanup() (err error) {
	for _, fpath := range s.tmpPaths {
		e := os.Remove(fpath)
		if e != nil {
			err = e
		}
	}
	return err
}

func (s *bulkSorter) flushBuffer() {
	// TO DO: use a context for error propagation?
	s.m.Lock() // only one buffer flush at a time
	f, err := ioutil.TempFile("", "kv_sort_shard")
	if err != nil {
		log.Fatalln(err)
	}
	go func(buf []*KeyValue) {
		defer s.m.Unlock()
		sort.Sort(ByKey(buf))
		s.tmpPaths = append(s.tmpPaths, f.Name())
		defer f.Close()
		var w *gzip.Writer
		w = gzip.NewWriter(f)
		enc := gob.NewEncoder(w)
		for _, x := range buf {
			err = enc.Encode(x)
			if err != nil {
				log.Fatalln(err)
			}
			s.entityLocation[string(x.Key)] = len(s.tmpPaths) - 1
		}
		w.Close()
	}(s.buffer[0:s.i])
	s.buffer = make([]*KeyValue, s.batchSize)
	s.i = 0
	return
}

func (s *bulkSorter) recordsFromFile(i int) chan *KeyValue {
	f, err := os.Open(s.tmpPaths[i])
	if err != nil {
		log.Fatalln(err)
	}
	var r *gzip.Reader
	r, err = gzip.NewReader(f)
	if err != nil {
		log.Fatalln(err)
	}
	dec := gob.NewDecoder(r)
	outCh := make(chan *KeyValue, 1)
	go func() {
		defer f.Close()
		for {
			var x KeyValue
			err = dec.Decode(&x)
			if err != nil {
				break
			}
			outCh <- &x
		}
		close(outCh)
		return
	}()
	return outCh
}

func (s *bulkSorter) sortedRecords() chan *KeyValue {
	s.m.RLock()
	outCh := make(chan *KeyValue, 1)
	channelsOfKeyValues := make(map[int]chan *KeyValue)
	for i := 0; i < len(s.tmpPaths); i++ {
		channelsOfKeyValues[i] = s.recordsFromFile(i)
	}
	go func() {
		defer s.m.RUnlock()
		keys := make(sort.StringSlice, len(s.entityLocation))
		i := 0
		for k := range s.entityLocation {
			keys[i] = k
			i++
		}
		keys.Sort()
		for _, k := range keys {
			loc, ok := s.entityLocation[k]
			if !ok {
				log.Fatalln("Missing key location:", k)
			}
			outCh <- <-channelsOfKeyValues[loc]
		}
		close(outCh)
	}()
	return outCh
}
