// Package chmap ConcurrentHashMap
package chmap

import (
	"errors"
	"hash/fnv"
	"sync"
)

const defaultCapacity = 16
const treeThreshold = 16

type node[v any] struct {
	hash  uint32
	key   string
	value v
	right *node[v]
	left  *node[v]
}

type bucket[v any] struct {
	sync.RWMutex
	node *node[v]
	tree bool
	size int64
}

// ConcurrentHashMap thread-safe string:any map
type ConcurrentHashMap[v any] struct {
	capacity uint32
	table    []*bucket[v]
}

// New returns ConcurrentHashMap with default capacity.
func New[v any]() ConcurrentHashMap[v] {
	chm, _ := NewWithCap[v](defaultCapacity)
	return chm
}

// NewWithCap returns ConcurrentHashMap with given capacity.
func NewWithCap[v any](capacity int) (chm ConcurrentHashMap[v], err error) {
	if capacity <= 0 {
		err = errors.New("capacity must be positive value")
		return
	}
	chm.capacity = uint32(capacity)
	chm.table = make([]*bucket[v], chm.capacity)
	for i := 0; i < int(chm.capacity); i++ {
		chm.table[i] = &bucket[v]{}
	}
	return
}

// Put maps the given key to the value, and saves the entry.
// In case of there is already an entry mapped by the given key, it updates the value of the entry.
func (m *ConcurrentHashMap[v]) Put(key string, value v) {
	h := hash(key)
	b := m.table[h%m.capacity]
	b.Lock()
	b.put(h, key, value)
	b.Unlock()
}

// Get returns value of the entry mapped by given key.
// If there is mopping by given key, it returns false.
func (m *ConcurrentHashMap[v]) Get(key string) (any, bool) {
	h := hash(key)
	b := m.table[h%m.capacity]
	b.RLock()
	n := b.get(h, key)
	b.RUnlock()
	if n == nil {
		return nil, false
	}
	return n.value, true
}

// GetOrDefault returns the value of the entry mapped by the given key.
// If there is mopping by the given key, it returns default value argument.
func (m *ConcurrentHashMap[v]) GetOrDefault(key string, defVal v) v {
	h := hash(key)
	b := m.table[h%m.capacity]
	b.RLock()
	n := b.get(h, key)
	b.RUnlock()
	if n == nil {
		return defVal
	}
	return n.value
}

// Contains returns if there is an entry mapped by the given key.
func (m *ConcurrentHashMap[v]) Contains(key string) bool {
	h := hash(key)
	b := m.table[h%m.capacity]
	b.RLock()
	n := b.get(h, key)
	b.RUnlock()
	return n != nil
}

// Remove removes the entry mapped by the given key and returns value of removed entry and true.
// In case of there is entry by the given key, It returns nil and false.
func (m *ConcurrentHashMap[v]) Remove(key string) (v, bool) {
	h := hash(key)
	b := m.table[h%m.capacity]
	b.Lock()
	n := b.remove(h, key)
	b.Unlock()
	if n == nil {
		return *new(v), false
	}
	return n.value, true
}

// Size returns the count of entries in the map
func (m *ConcurrentHashMap[v]) Size() int {
	var size int64 = 0
	for _, b := range m.table {
		size += b.size
	}
	return int(size)
}

func (b *bucket[v]) get(h uint32, k string) *node[v] {
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

func (b *bucket[v]) put(h uint32, k string, val v) {
	if fn := b.get(h, k); fn != nil {
		fn.value = val
		return
	}
	nn := &node[v]{
		hash:  h,
		key:   k,
		value: val,
	}
	if b.node == nil {
		b.node = nn
		b.size = 1
		return
	}
	if b.tree {
		if treePut(b.node, nn) {
			b.size++
		}
	} else {
		if listPut(b.node, nn) {
			b.size++
		}
		if b.size >= treeThreshold {
			r := treeify(b.node)
			b.node = r
			b.tree = true
		}
	}
}

func (b *bucket[v]) remove(h uint32, k string) (rn *node[v]) {
	if b.tree {
		var sn *node[v]
		sn, rn = treeRemove(b.node, h, k)
		if rn != nil {
			b.size--
			if b.node == rn {
				b.node = sn
			}
		}
	} else {
		var ok bool
		rn, ok = listRemove(b.node, h, k)
		if ok {
			b.size--
			if rn == nil {
				rn = b.node
				if b.node.right != nil {
					b.node = b.node.right
				} else {
					b.node = nil
				}
			}
		}
	}
	return rn
}

func hash(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}

func treeify[v any](head *node[v]) (root *node[v]) {
	nodes := collect(head)
	sort(nodes)
	ri := len(nodes) / 2
	root = nodes[ri]
	split(nodes[:ri], root, true)
	split(nodes[ri+1:], root, false)
	return
}

func split[v any](nodes []*node[v], root *node[v], left bool) {
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

func collect[v any](head *node[v]) []*node[v] {
	s := size(head)
	ns := make([]*node[v], s)
	n := head
	for i := 0; i < s; i++ {
		ns[i] = n
		n = n.right
	}
	return ns
}

func size[v any](head *node[v]) int {
	n := head
	s := 0
	for n != nil {
		s++
		n = n.right
	}
	return s
}

func sort[v any](nodes []*node[v]) {
	for i := 0; i < len(nodes)-1; i++ {
		for j := 0; j < len(nodes)-1-i; j++ {
			if nodes[j].hash > nodes[j+1].hash {
				nodes[j], nodes[j+1] = nodes[j+1], nodes[j]
			}
		}
	}
}

func listRemove[v any](n *node[v], h uint32, k string) (*node[v], bool) {
	var pn *node[v]
	for n != nil {
		if n.hash == h && n.key == k {
			if pn == nil {
				return nil, true
			}
			pn.right = n.right
			return n, true
		}
		pn = n
		n = n.right
	}
	return nil, false
}

func treeRemove[v any](r *node[v], h uint32, k string) (*node[v], *node[v]) {
	if r == nil {
		return nil, nil
	}
	if r.hash > h {
		var rn *node[v]
		r.left, rn = treeRemove(r.left, h, k)
		return r, rn
	} else if r.hash < h || r.key != k {
		var rn *node[v]
		r.right, rn = treeRemove(r.right, h, k)
		return r, rn
	}
	if r.left == nil {
		return r.right, r
	} else if r.right == nil {
		return r.left, r
	} else {
		spn := r
		sn := r.right
		for sn.left != nil {
			spn = sn
			sn = sn.left
		}
		if spn != r {
			spn.left = sn.right
		} else {
			spn.right = sn.right
		}
		rn := &node[v]{
			hash:  r.hash,
			key:   r.key,
			value: r.value,
		}
		r.hash = sn.hash
		r.key = sn.key
		r.value = sn.value
		return r, rn
	}
}

func listPut[v any](hn *node[v], nn *node[v]) bool {
	var pn *node[v]
	for hn != nil {
		if hn.hash == nn.hash && hn.key == nn.key {
			hn.value = nn.value
			return false
		}
		pn = hn
		hn = hn.right
	}
	if pn != nil {
		pn.right = nn
		return true
	}
	return false
}

func treePut[v any](rn *node[v], nn *node[v]) bool {
	var pn *node[v]
	for rn != nil {
		if rn.hash == nn.hash && rn.key == nn.key {
			rn.value = nn.value
			return false
		}
		pn = rn
		if rn.hash > nn.hash {
			rn = rn.left
		} else {
			rn = rn.right
		}
	}
	if pn.hash > nn.hash {
		pn.left = nn
	} else {
		pn.right = nn
	}
	return true
}
