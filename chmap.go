package chmap

import (
	"github.com/mitchellh/hashstructure/v2"
	"hash/fnv"
	"sync"
)

const defaultBucketCount = 16

type Hasher interface {
	Hash() uint64
}

type ConcurrentHashMap struct {
	bucketCount int
	buckets     []*Bucket
}

type Bucket struct {
	sync.RWMutex
	internal map[interface{}]interface{}
}

func New() (chm ConcurrentHashMap) {
	chm.bucketCount = defaultBucketCount
	chm.buckets = make([]*Bucket, defaultBucketCount)
	for i := 0; i < defaultBucketCount; i++ {
		chm.buckets[i] = &Bucket{internal: make(map[interface{}]interface{})}
	}
	return
}

func (m *ConcurrentHashMap) Put(key interface{}, value interface{}) {
	bucket := m.getBucket(key)
	bucket.Lock()
	defer bucket.Unlock()
	bucket.internal[key] = value
}

func (m *ConcurrentHashMap) Get(key interface{}) (val interface{}, ok bool) {
	bucket := m.getBucket(key)
	bucket.RLock()
	defer bucket.RUnlock()
	val, ok = bucket.internal[key]
	return
}

func (m *ConcurrentHashMap) Contains(key interface{}) (ok bool) {
	bucket := m.getBucket(key)
	bucket.RLock()
	defer bucket.RUnlock()
	_, ok = bucket.internal[key]
	return
}

func (m *ConcurrentHashMap) Count() int {
	count := 0
	for i := 0; i < m.bucketCount; i++ {
		bucket := m.getBucket(i)
		bucket.RLock()
		count += len(bucket.internal)
		bucket.RUnlock()
	}
	return count
}

func (m *ConcurrentHashMap) Remove(key interface{}) {
	bucket := m.getBucket(key)
	bucket.Lock()
	defer bucket.Unlock()
	delete(bucket.internal, key)
}

func (m *ConcurrentHashMap) Clear() {
	for i := 0; i < m.bucketCount; i++ {
		bucket := m.getBucket(i)
		bucket.Lock()
		for k := range bucket.internal {
			delete(bucket.internal, k)
		}
		bucket.Unlock()
	}
}

func (m *ConcurrentHashMap) getBucket(key interface{}) *Bucket {
	hash := getHash(key)
	return m.buckets[int(hash)&m.bucketCount]
}

func getHash(key interface{}) (hash uint64) {
	switch k := key.(type) {
	case Hasher:
		hash = k.Hash()
	case string:
		h := fnv.New32a()
		_, _ = h.Write([]byte(k))
		hash = uint64(h.Sum32())
	default:
		hash, _ = hashstructure.Hash(key, hashstructure.FormatV2, nil)
	}
	return
}
