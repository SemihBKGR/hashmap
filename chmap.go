package chmap

import (
	"hash/fnv"
	"sync"
)

const loadFactor = 0.75
const defaultCapacity = 16
const treeThreshold = 16

type node struct {
	hash  uint32
	key   string
	value interface{}
	right *node
	left  *node
}

type bucket struct {
	sync.RWMutex
	node *node
	tree bool
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
	b := m.buckets[h%m.capacity]
	b.Lock()
	defer b.Unlock()
	n := &node{
		hash:  h,
		key:   key,
		value: value,
		right: nil,
	}
	if b.tree {
		l := findLeaf(b.node, h)
		if l == nil {
			b.node = n
		} else {
			if l.hash > h {
				l.left = n
			} else {
				l.right = n
			}
		}
	} else {
		t, i := findTail(b.node)
		if t == nil {
			b.node = n
		} else {
			t.right = n
			if i >= treeThreshold {
				//TODO: make linked list tree
			}
		}
	}
}

func (m *ConcurrentHashMap) Get(key string) (val interface{}, ok bool) {
	h := hash(key)
	b := m.buckets[h%m.capacity]
	b.RLock()
	defer b.RUnlock()
	n := b.node
	for n != nil {
		if n.key == key {
			return n.value, true
		}
		if b.tree && n.hash > h {
			n = n.left
		} else {
			n = n.right
		}
	}
	return nil, false
}

func (m *ConcurrentHashMap) Contains(key string) bool {
	h := hash(key)
	b := m.buckets[h%m.capacity]
	b.RLock()
	defer b.RUnlock()
	n := b.node
	for n != nil {
		if n.key == key {
			return true
		}
		if b.tree && n.hash > h {
			n = n.left
		} else {
			n = n.right
		}
	}
	return false
}

func (m *ConcurrentHashMap) Remove(key string) (val interface{}, ok bool) {
	h := hash(key)
	b := m.buckets[h%m.capacity]
	b.RLock()
	defer b.RUnlock()
	if b.tree {
		n := b.node
		var pn *node
		for n != nil {
			if n.key == key {
				if n.left != nil {
					n = n.left
					var ipn *node
					for {
						if n.right != nil {
							ipn = n
							n = n.right
						} else if n.left != nil {
							ipn = n
							n = n.left
						} else {
							break
						}
					}
					pn.value = n.value
					if ipn.left == n {
						ipn.left = nil
					} else {
						ipn.right = nil
					}
				} else if n.right != nil {

				} else {
					if pn.left == n {
						pn.left = nil
					} else {
						pn.right = nil
					}
				}
			}
			pn = n
			if n.hash > h {
				n = n.left
			} else {
				n = n.right
			}
		}
		return nil, false
	} else {
		n := b.node
		var pn *node
		for n != nil {
			if n.key == key {
				if pn == nil {
					b.node = nil
					return n.value, true
				}
				pn.right = n.right
				return n.value, true
			}
			pn = n
			n = n.right
		}
		return nil, false
	}
}

func hash(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}

func findTail(n *node) (*node, int) {
	if n == nil {
		return nil, 0
	}
	i := 1
	for n.right != nil {
		i++
		n = n.right
	}
	return n, i
}

func findLeaf(n *node, hash uint32) *node {
	if n == nil {
		return nil
	}
	var l *node
	for n != nil {
		l = n
		if n.hash > hash {
			n = n.left
		} else {
			n = n.right
		}
	}
	return l
}
