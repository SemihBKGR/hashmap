package chmap

import (
	"fmt"
	"hash/fnv"
	"sync"
)

const loadFactor = 0.75
const defaultCapacity = 16

type node struct {
	hash  uint32
	key   string
	value interface{}
	next  *node
}

type bucket struct {
	sync.RWMutex
	node *node
}

type ConcurrentHashMap struct {
	capacity   uint32
	loadFactor float32
	buckets    []*bucket
}

func New() (chm ConcurrentHashMap) {
	chm.capacity = defaultCapacity
	chm.loadFactor = loadFactor
	chm.buckets = make([]*bucket, chm.capacity)
	for i := 0; i < int(chm.capacity); i++ {
		chm.buckets[i] = &bucket{}
	}
	return
}

func (m *ConcurrentHashMap) Put(key string, value interface{}) {
	h := hash(key)
	fmt.Println(h)
	b := m.buckets[h%m.capacity]
	b.Lock()
	defer b.Unlock()
	if b.node == nil {
		b.node = &node{
			hash:  h,
			key:   key,
			value: value,
			next:  nil,
		}
	} else {
		n := b.node
		for n.next != nil {
			n = n.next
		}
		n.next = &node{
			hash:  h,
			key:   key,
			value: value,
			next:  nil,
		}
	}
}

func (m *ConcurrentHashMap) Get(key string) (val interface{}, ok bool) {
	h := hash(key)
	fmt.Println(h)
	b := m.buckets[h%m.capacity]
	b.RLock()
	defer b.RUnlock()
	n := b.node
	for n != nil {
		if n.key == key {
			fmt.Println("found")
			return n.value, true
		}
		n = n.next
	}
	fmt.Println("not found")
	return nil, false
}

func hash(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}
