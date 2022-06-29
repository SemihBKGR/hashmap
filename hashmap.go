// Package hashmap concurrent hash map
package hashmap

import (
	"errors"
	"hash/fnv"
	"sync"
)

const (
	defaultCapacity  = 16
	treeifyThreshold = 16
)

type node[k, v any] struct {
	hash  uint32
	key   k
	value v
	right *node[k, v]
	left  *node[k, v]
}

type bucket[k, v any] struct {
	sync.RWMutex
	node *node[k, v]
	tree bool
	size int64
}

// Hasher general interface to provide hash and equals function
type Hasher interface {
	Hash() uint32
	Equals(a any) bool
}

type HashFunc[k any] func(key k) uint32

type EqualsFunc[k any] func(k1, k2 k) bool

// ConcurrentHashMap thread-safe string:any map
type ConcurrentHashMap[k, v any] struct {
	capacity uint32
	table    []*bucket[k, v]
	hf       HashFunc[k]
	ef       EqualsFunc[k]
}

// New returns ConcurrentHashMap with default capacity.
func New[k Hasher, v any]() ConcurrentHashMap[k, v] {
	chm, _ := NewWithCap[k, v](defaultCapacity)
	return chm
}

// NewString returns string type key ConcurrentHashMap with default capacity.
func NewString[v any]() ConcurrentHashMap[string, v] {
	chm, _ := NewStringWithCap[v](defaultCapacity)
	return chm
}

// NewWithFuncs returns ConcurrentHashMap with the given funcs and default capacity.
func NewWithFuncs[k, v any](hf HashFunc[k], ef EqualsFunc[k]) (ConcurrentHashMap[k, v], error) {
	return NewWithCapAndFuncs[k, v](defaultCapacity, hf, ef)
}

// NewWithCap returns ConcurrentHashMap with given capacity.
func NewWithCap[k Hasher, v any](capacity int) (chm ConcurrentHashMap[k, v], err error) {
	hf := func(key k) uint32 {
		return key.Hash()
	}
	ef := func(k1, k2 k) bool {
		return k1.Equals(k2)
	}
	chm, err = NewWithCapAndFuncs[k, v](capacity, hf, ef)
	return
}

// NewStringWithCap returns string type key ConcurrentHashMap with given capacity.
func NewStringWithCap[v any](capacity int) (chm ConcurrentHashMap[string, v], err error) {
	hf := func(key string) uint32 {
		h := fnv.New32a()
		_, _ = h.Write([]byte(key))
		return h.Sum32()
	}
	ef := func(k1, k2 string) bool {
		return k1 == k2
	}
	chm, err = NewWithCapAndFuncs[string, v](capacity, hf, ef)
	return
}

// NewWithCapAndFuncs returns ConcurrentHashMap with the given capacity and funcs.
func NewWithCapAndFuncs[k, v any](capacity int, hf HashFunc[k], ef EqualsFunc[k]) (chm ConcurrentHashMap[k, v], err error) {
	if capacity <= 0 {
		err = errors.New("capacity must be positive value")
		return
	}
	if hf == nil || ef == nil {
		err = errors.New("hash and equals funcs cannot be nil")
		return
	}
	chm.capacity = uint32(capacity)
	chm.table = make([]*bucket[k, v], chm.capacity)
	for i := 0; i < int(chm.capacity); i++ {
		chm.table[i] = &bucket[k, v]{}
	}
	chm.hf = hf
	chm.ef = ef
	return
}

// Put maps the given key to the value, and saves the entry.
// In case of there is already an entry mapped by the given key, it updates the value of the entry.
func (m *ConcurrentHashMap[k, v]) Put(key k, val v) {
	h := m.hf(key)
	b := m.table[h%m.capacity]
	b.Lock()
	b.put(h, key, val, m.ef)
	b.Unlock()
}

// Get returns value of the entry mapped by given key.
// If there is mopping by given key, it returns false.
func (m *ConcurrentHashMap[k, v]) Get(key k) (v, bool) {
	h := m.hf(key)
	b := m.table[h%m.capacity]
	b.RLock()
	n := b.get(h, key, m.ef)
	b.RUnlock()
	if n == nil {
		return *new(v), false
	}
	return n.value, true
}

// GetOrDefault returns the value of the entry mapped by the given key.
// If there is mopping by the given key, it returns default value argument.
func (m *ConcurrentHashMap[k, v]) GetOrDefault(key k, defVal v) v {
	h := m.hf(key)
	b := m.table[h%m.capacity]
	b.RLock()
	n := b.get(h, key, m.ef)
	b.RUnlock()
	if n == nil {
		return defVal
	}
	return n.value
}

// Contains returns if there is an entry mapped by the given key.
func (m *ConcurrentHashMap[k, v]) Contains(key k) bool {
	h := m.hf(key)
	b := m.table[h%m.capacity]
	b.RLock()
	n := b.get(h, key, m.ef)
	b.RUnlock()
	return n != nil
}

// Remove removes the entry mapped by the given key and returns value of removed entry and true.
// In case of there is entry by the given key, It returns nil and false.
func (m *ConcurrentHashMap[k, v]) Remove(key k) (v, bool) {
	h := m.hf(key)
	b := m.table[h%m.capacity]
	b.Lock()
	n := b.remove(h, key, m.ef)
	b.Unlock()
	if n == nil {
		return *new(v), false
	}
	return n.value, true
}

// Size returns the count of entries in the map
func (m *ConcurrentHashMap[k, v]) Size() int {
	var size int64 = 0
	for _, b := range m.table {
		size += b.size
	}
	return int(size)
}

func (b *bucket[k, v]) get(h uint32, key k, ef EqualsFunc[k]) *node[k, v] {
	n := b.node
	for n != nil {
		if n.hash == h && ef(n.key, key) {
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

func (b *bucket[k, v]) put(h uint32, key k, val v, ef EqualsFunc[k]) {
	if fn := b.get(h, key, ef); fn != nil {
		fn.value = val
		return
	}
	nn := &node[k, v]{
		hash:  h,
		key:   key,
		value: val,
	}
	if b.node == nil {
		b.node = nn
		b.size = 1
		return
	}
	if b.tree {
		if treePut(b.node, nn, ef) {
			b.size++
		}
	} else {
		if listPut(b.node, nn, ef) {
			b.size++
		}
		if b.size >= treeifyThreshold {
			r := treeify(b.node)
			b.node = r
			b.tree = true
		}
	}
}

func (b *bucket[k, v]) remove(h uint32, key k, ef EqualsFunc[k]) (rn *node[k, v]) {
	if b.tree {
		var sn *node[k, v]
		sn, rn = treeRemove(b.node, h, key, ef)
		if rn != nil {
			b.size--
			if b.node == rn {
				b.node = sn
			}
		}
	} else {
		var ok bool
		rn, ok = listRemove(b.node, h, key, ef)
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

func treeify[k, v any](head *node[k, v]) (root *node[k, v]) {
	nodes := collect(head)
	sort(nodes)
	ri := len(nodes) / 2
	root = nodes[ri]
	split(nodes[:ri], root, true)
	split(nodes[ri+1:], root, false)
	return
}

func split[k, v any](nodes []*node[k, v], root *node[k, v], left bool) {
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

func collect[k, v any](head *node[k, v]) []*node[k, v] {
	s := size(head)
	ns := make([]*node[k, v], s)
	n := head
	for i := 0; i < s; i++ {
		ns[i] = n
		n = n.right
	}
	return ns
}

func size[k, v any](head *node[v, k]) int {
	n := head
	s := 0
	for n != nil {
		s++
		n = n.right
	}
	return s
}

func sort[k, v any](nodes []*node[k, v]) {
	for i := 0; i < len(nodes)-1; i++ {
		for j := 0; j < len(nodes)-1-i; j++ {
			if nodes[j].hash > nodes[j+1].hash {
				nodes[j], nodes[j+1] = nodes[j+1], nodes[j]
			}
		}
	}
}

func listRemove[k, v any](n *node[k, v], h uint32, key k, ef EqualsFunc[k]) (*node[k, v], bool) {
	var pn *node[k, v]
	for n != nil {
		if n.hash == h && ef(n.key, key) {
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

func treeRemove[k, v any](r *node[k, v], h uint32, key k, ef EqualsFunc[k]) (*node[k, v], *node[k, v]) {
	if r == nil {
		return nil, nil
	}
	if r.hash > h {
		var rn *node[k, v]
		r.left, rn = treeRemove(r.left, h, key, ef)
		return r, rn
	} else if r.hash < h || !ef(r.key, key) {
		var rn *node[k, v]
		r.right, rn = treeRemove(r.right, h, key, ef)
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
		rn := &node[k, v]{
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

func listPut[k, v any](hn *node[k, v], nn *node[k, v], ef EqualsFunc[k]) bool {
	var pn *node[k, v]
	for hn != nil {
		if hn.hash == nn.hash && ef(hn.key, nn.key) {
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

func treePut[k, v any](rn *node[k, v], nn *node[k, v], ef EqualsFunc[k]) bool {
	var pn *node[k, v]
	for rn != nil {
		if rn.hash == nn.hash && ef(rn.key, nn.key) {
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
