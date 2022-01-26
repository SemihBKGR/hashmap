package chmap

import (
	"sync"
)

const defaultBucketCount = 16

type Hasher func(interface{}) int

type ConcurrentHashMap struct {
	bucketCount int
	buckets     []*Bucket
	hasher      Hasher
}

type Bucket struct {
	sync.RWMutex
	internal map[interface{}]interface{}
}

func New(hasher Hasher) (chm ConcurrentHashMap) {
	chm.hasher = hasher
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
	hash := m.hasher(key)
	return m.buckets[hash%m.bucketCount]
}
