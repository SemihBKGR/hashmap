// Package chmap concurrent hash map implemention
package chmap

import (
	"errors"
	"hash/fnv"
	"sync"
)

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
	size int
}

// ConcurrentHashMap string:any map
type ConcurrentHashMap struct {
	capacity uint32
	table    []*bucket
}

// New returns ConcurrentHashMap with default capacity
func New() ConcurrentHashMap {
	chm, _ := NewWithCap(defaultCapacity)
	return chm
}

// NewWithCap returns ConcurrentHashMap with given capacity
func NewWithCap(capacity int) (chm ConcurrentHashMap, err error) {
	if capacity <= 0 {
		err = errors.New("capacity must be positive value")
		return
	}
	chm.capacity = uint32(capacity)
	chm.table = make([]*bucket, chm.capacity)
	for i := 0; i < int(chm.capacity); i++ {
		chm.table[i] = &bucket{}
	}
	return
}

// Put maps given key to given value
func (m *ConcurrentHashMap) Put(key string, value interface{}) {
	h := hash(key)
	b := m.table[h%m.capacity]
	b.Lock()
	b.put(h, key, value)
	b.Unlock()
}

// Get returns mapped value fo given key
func (m *ConcurrentHashMap) Get(key string) (interface{}, bool) {
	h := hash(key)
	b := m.table[h%m.capacity]
	b.RLock()
	n := b.get(h, key)
	b.RUnlock()
	if n != nil {
		return n.value, true
	} else {
		return nil, false
	}
}

// GetOrDefault returns the value mapped by the given key
// If there isn't any mapping by given key, it returns given defaul value
func (m *ConcurrentHashMap) GetOrDefault(key string, defVal interface{}) interface{} {
	h := hash(key)
	b := m.table[h%m.capacity]
	b.RLock()
	n := b.get(h, key)
	b.RUnlock()
	if n != nil {
		return n.value
	} else {
		return defVal
	}
}

// Contains returns if given key is mapped or not
func (m *ConcurrentHashMap) Contains(key string) bool {
	h := hash(key)
	b := m.table[h%m.capacity]
	b.RLock()
	n := b.get(h, key)
	b.RUnlock()
	return n != nil
}

// Remove removes entry by given key
func (m *ConcurrentHashMap) Remove(key string) (val interface{}, ok bool) {
	h := hash(key)
	b := m.table[h%m.capacity]
	b.RLock()
	n := b.remove(h, key)
	b.RUnlock()
	if n != nil {
		return n.value, true
	} else {
		return nil, false
	}
}

// Size returns size of the map
func (m *ConcurrentHashMap) Size() int {
	size := 0
	for _, b := range m.table {
		size += b.size
	}
	return size
}

func hash(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}

func treeify(head *node) (root *node) {
	nodes := collect(head)
	sort(nodes)
	ri := len(nodes) / 2
	root = nodes[ri]
	split(nodes[:ri], root, true)
	split(nodes[ri+1:], root, false)
	return
}

func split(nodes []*node, root *node, left bool) {
	l := len(nodes)
	if l == 0 {
		if left {
			root.left = nil
		} else {
			root.right = nil
		}
		return
	}
	ri := len(nodes) / 2
	if left {
		root.left = nodes[ri]
	} else {
		root.right = nodes[ri]
	}
	split(nodes[:ri], nodes[ri], true)
	split(nodes[ri+1:], nodes[ri], false)
}

func collect(head *node) []*node {
	s := size(head)
	ns := make([]*node, s)
	n := head
	for i := 0; i < s; i++ {
		ns[i] = n
		n = n.right
	}
	return ns
}

func size(head *node) int {
	n := head
	s := 0
	for n != nil {
		s++
		n = n.right
	}
	return s
}

func sort(nodes []*node) {
	for i := 0; i < len(nodes)-1; i++ {
		for j := 0; j < len(nodes)-1-i; j++ {
			if nodes[j].hash > nodes[j+1].hash {
				nodes[j], nodes[j+1] = nodes[j+1], nodes[j]
			}
		}
	}
}

func (b *bucket) get(h uint32, k string) *node {
	n := b.node
	for n != nil {
		if n.hash == h && n.key == k {
			return n
		}
		if b.tree && n.hash > h {
			n = n.left
		} else {
			n = n.right
		}
	}
	return nil
}

func (b *bucket) put(h uint32, k string, v interface{}) {
	nn := &node{
		hash:  h,
		key:   k,
		value: v,
	}
	if b.tree {
		n := b.node
		if n == nil {
			b.node = n
			b.size++
			return
		}
		var pn *node
		for n != nil {
			if n.hash == h && n.key == k {
				n.value = v
				return
			}
			pn = n
			if n.hash > h {
				n = n.left
			} else {
				n = n.right
			}
		}
		if pn.hash > h {
			pn.left = nn
		} else {
			pn.right = nn
		}
		b.size++
	} else {
		fn := b.get(h, k)
		if fn != nil {
			fn.value = v
		} else {
			nn.right = b.node
			b.node = nn
			b.size++
			if b.size >= treeThreshold {
				treeify(b.node)
				b.tree = true
			}
		}
	}
}

func (b *bucket) remove(h uint32, k string) *node {
	if b.tree {
		n := b.node
		var pn *node
		for n != nil {
			if n.hash == h && n.key == k {
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
					n = n.right
					var ipn *node
					for {
						if n.left != nil {
							ipn = n
							n = n.left
						} else if n.right != nil {
							ipn = n
							n = n.right
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
	} else {
		n := b.node
		var pn *node
		for n != nil {
			if n.hash == h && n.key == k {
				if pn == nil {
					pn = b.node
					b.node = nil
					b.size--
					return pn
				} else {
					pn.right = n.right
					b.size--
					return n
				}
			}
			pn = n
			n = n.right
		}
	}
	return nil
}
