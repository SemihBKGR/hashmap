package chmap

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
)

func TestNew(t *testing.T) {
	_ = New()
}

func TestNewWithCap_PositiveCapacity(t *testing.T) {
	_, err := NewWithCap(32)
	if err != nil {
		t.FailNow()
	}
}

func TestNewWithCap_ZeroCapacity(t *testing.T) {
	_, err := NewWithCap(0)
	if err == nil {
		t.FailNow()
	}
}

func TestNewWithCap_NegativeCapacity(t *testing.T) {
	_, err := NewWithCap(-1)
	if err == nil {
		t.FailNow()
	}
}

func TestConcurrentHashMap_Put(t *testing.T) {
	m := New()
	for i := 0; i < 100_000; i++ {
		m.Put(strconv.Itoa(i), i)
	}
	for i := 50_000; i < 150_000; i++ {
		m.Put(strconv.Itoa(i), i)
	}
}

func TestConcurrentHashMap_ConcurrentlyPut(t *testing.T) {
	m := New()
	var wg sync.WaitGroup
	putItr := func(m *ConcurrentHashMap, wg *sync.WaitGroup, from, to int) {
		defer wg.Done()
		for i := from; i < to; i++ {
			m.Put(strconv.Itoa(i), i)
		}
	}
	wg.Add(3)
	go putItr(&m, &wg, 0, 100_000)
	go putItr(&m, &wg, 50_000, 150_000)
	go putItr(&m, &wg, 100_000, 200_000)
	wg.Wait()
}

func TestConcurrentHashMap_Put_Get(t *testing.T) {
	m := New()
	for i := 0; i < 100_000; i++ {
		k := strconv.Itoa(i)
		m.Put(k, i)
		if v, ok := m.Get(k); !ok || v != i {
			t.FailNow()
		}
	}
	for i := 100_000; i < 150_000; i++ {
		k := strconv.Itoa(i)
		if _, ok := m.Get(k); ok {
			t.FailNow()
		}
	}
}

func TestConcurrentHashMap_ConcurrentlyPut_Get(t *testing.T) {
	m := New()
	var wg sync.WaitGroup
	putRange := func(m *ConcurrentHashMap, wg *sync.WaitGroup, from, to int) {
		defer wg.Done()
		for i := from; i < to; i++ {
			m.Put(strconv.Itoa(i), i)
		}
	}
	wg.Add(3)
	go putRange(&m, &wg, 0, 100_000)
	go putRange(&m, &wg, 50_000, 150_000)
	go putRange(&m, &wg, 100_000, 200_000)
	wg.Wait()
	for i := 0; i < 200_000; i++ {
		k := strconv.Itoa(i)
		if v, ok := m.Get(k); !ok || v != i {
			t.Errorf("key: %s, value: %v, ok: %v", k, v, ok)
			t.FailNow()
		}
	}
}

func TestConcurrentHashMap_Put_GetOrDefault(t *testing.T) {
	m := New()
	for i := 0; i < 100_000; i++ {
		k := strconv.Itoa(i)
		m.Put(k, i)
		if v := m.GetOrDefault(k, nil); v != i {
			t.FailNow()
		}
	}
	for i := 100_000; i < 150_000; i++ {
		k := strconv.Itoa(i)
		if v := m.GetOrDefault(k, nil); v != nil {
			t.FailNow()
		}
	}
}

func TestConcurrentHashMap_ConcurrentlyPut_GetOrDefault(t *testing.T) {
	m := New()
	var wg sync.WaitGroup
	putRange := func(m *ConcurrentHashMap, wg *sync.WaitGroup, from, to int) {
		defer wg.Done()
		for i := from; i < to; i++ {
			m.Put(strconv.Itoa(i), i)
		}
	}
	wg.Add(3)
	go putRange(&m, &wg, 0, 100_000)
	go putRange(&m, &wg, 50_000, 150_000)
	go putRange(&m, &wg, 100_000, 200_000)
	wg.Wait()
	for i := 0; i < 200_000; i++ {
		if v := m.GetOrDefault(strconv.Itoa(i), nil); v != i {
			t.FailNow()
		}
	}
	for i := 200_000; i < 300_000; i++ {
		if v := m.GetOrDefault(strconv.Itoa(i), nil); v != nil {
			t.FailNow()
		}
	}
}

func TestConcurrentHashMap_Put_Contains(t *testing.T) {
	m := New()
	for i := 0; i < 100_000; i++ {
		k := strconv.Itoa(i)
		m.Put(k, i)
		if ok := m.Contains(k); !ok {
			t.FailNow()
		}
	}
	for i := 100_000; i < 150_000; i++ {
		if ok := m.Contains(strconv.Itoa(i)); ok {
			t.FailNow()
		}
	}
}

func TestConcurrentHashMap_ConcurrentlyPut_Contains(t *testing.T) {
	m := New()
	var wg sync.WaitGroup
	putRange := func(m *ConcurrentHashMap, wg *sync.WaitGroup, from, to int) {
		defer wg.Done()
		for i := from; i < to; i++ {
			m.Put(strconv.Itoa(i), i)
		}
	}
	wg.Add(3)
	go putRange(&m, &wg, 0, 100_000)
	go putRange(&m, &wg, 50_000, 150_000)
	go putRange(&m, &wg, 100_000, 200_000)
	wg.Wait()
	for i := 0; i < 200_000; i++ {
		if ok := m.Contains(strconv.Itoa(i)); !ok {
			t.FailNow()
		}
	}
	for i := 200_000; i < 300_000; i++ {
		if ok := m.Contains(strconv.Itoa(i)); ok {
			t.FailNow()
		}
	}
}

/*
func TestConcurrentHashMap_Put_Remove(t *testing.T) {
	m := New()
	for i := 0; i < 100_000; i++ {
		k := strconv.Itoa(i)
		m.Put(k, i)
	}
	for i := 0; i < 100_000; i++ {
		if val, ok := m.Remove(strconv.Itoa(i)); !ok && val != i {
			t.Logf("i: %d, val: %v, ok: %v", i, val, ok)
			t.FailNow()
		}
	}
	for i := 0; i < 200_000; i++ {
		if _, ok := m.Remove(strconv.Itoa(i)); ok {
			t.FailNow()
		}
	}
}
*/

/*
func TestConcurrentHashMap_ConcurrentlyPut_Remove(t *testing.T) {
	m := New()
	var wg sync.WaitGroup
	putRange := func(m *ConcurrentHashMap, wg *sync.WaitGroup, from, to int) {
		defer wg.Done()
		for i := from; i < to; i++ {
			m.Put(strconv.Itoa(i), i)
		}
	}
	wg.Add(3)
	go putRange(&m, &wg, 0, 100_000)
	go putRange(&m, &wg, 50_000, 150_000)
	go putRange(&m, &wg, 100_000, 200_000)
	wg.Wait()
	for i := 0; i < 200_000; i++ {
		if val, ok := m.Remove(strconv.Itoa(i)); !ok || val != i {
			t.Logf("i: %d, val: %v, ok: %v", i, val, ok)
			t.FailNow()
		}
	}
	for i := 0; i < 300_000; i++ {
		if _, ok := m.Remove(strconv.Itoa(i)); ok {
			t.FailNow()
		}
	}
}
*/

/*
func TestConcurrentHashMap_Put_Remove_Contains(t *testing.T) {
	m := New()
	for i := 0; i < 100_000; i++ {
		k := strconv.Itoa(i)
		m.Put(k, i)
		if val, ok := m.Remove(k); !ok || val != i {
			t.FailNow()
		}
		if ok := m.Contains(strconv.Itoa(i)); ok {
			t.FailNow()
		}
	}
	for i := 100_000; i < 150_000; i++ {
		k := strconv.Itoa(i)
		if _, ok := m.Remove(k); ok {
			t.FailNow()
		}
	}
}
*/

func TestConcurrentHashMap_Put_Size(t *testing.T) {
	m := New()
	for i := 0; i < 100_000; i++ {
		m.Put(strconv.Itoa(i), i)
	}
	if m.Size() != 100_000 {
		t.FailNow()
	}
	for i := 50_000; i < 150_000; i++ {
		m.Put(strconv.Itoa(i), i)
	}
	if m.Size() != 150_000 {
		t.FailNow()
	}
}

func TestConcurrentHashMap_ConcurrentlyPut_Size(t *testing.T) {
	m := New()
	var wg sync.WaitGroup
	putItr := func(m *ConcurrentHashMap, wg *sync.WaitGroup, from, to int) {
		defer wg.Done()
		for i := from; i < to; i++ {
			m.Put(strconv.Itoa(i), i)
		}
	}
	wg.Add(3)
	go putItr(&m, &wg, 0, 100_000)
	go putItr(&m, &wg, 50_000, 150_000)
	go putItr(&m, &wg, 100_000, 200_000)
	wg.Wait()
	if s := m.Size(); s != 200_000 {
		t.Errorf("size: %d", s)
		t.FailNow()
	}
}

func TestTreeify(t *testing.T) {
	var head *node
	n := &node{
		hash: rand.Uint32(),
	}
	head = n
	for i := 0; i < 9; i++ {
		n.right = &node{
			hash: rand.Uint32(),
		}
		n = n.right
	}
	root := treeify(head)
	if !isTreeRootAsExpected(root) {
		t.FailNow()
	}
}

func isTreeRootAsExpected(r *node) bool {
	if r == nil {
		return true
	}
	return isTreeLeafAsExpected(r) && isTreeRootAsExpected(r.left) && isTreeRootAsExpected(r.right)
}

func isTreeLeafAsExpected(n *node) bool {
	if n == nil {
		return true
	}
	if n.right != nil && n.left != nil {
		return n.hash < n.right.hash && n.hash > n.left.hash && n.right.hash > n.left.hash
	} else if n.right != nil {
		return n.hash < n.right.hash
	} else if n.left != nil {
		return n.hash > n.left.hash
	} else {
		return true
	}
}
