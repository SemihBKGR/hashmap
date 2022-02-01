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

func TestConcurrentHashMap_Put_Verify(t *testing.T) {
	m := New()
	for i := 0; i < 10_000; i++ {
		m.Put(strconv.Itoa(i), i)
	}
	for i := 5_000; i < 15_000; i++ {
		m.Put(strconv.Itoa(i), i)
	}
	verifyMap(t, &m)
}

func TestConcurrentHashMap_ConcurrentlyPut(t *testing.T) {
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
}

func TestConcurrentHashMap_ConcurrentlyPut_Verify(t *testing.T) {
	m := New()
	var wg sync.WaitGroup
	putRange := func(m *ConcurrentHashMap, wg *sync.WaitGroup, from, to int) {
		defer wg.Done()
		for i := from; i < to; i++ {
			m.Put(strconv.Itoa(i), i)
		}
	}
	wg.Add(3)
	go putRange(&m, &wg, 0, 10_000)
	go putRange(&m, &wg, 5_000, 15_000)
	go putRange(&m, &wg, 10_000, 20_000)
	wg.Wait()
	verifyMap(t, &m)
}

func TestConcurrentHashMap_Put_Get(t *testing.T) {
	m := New()
	for i := 0; i < 100_000; i++ {
		k := strconv.Itoa(i)
		m.Put(k, i)
		if v, ok := m.Get(k); !ok || v != i {
			t.Logf("key: %s, value: %v, ok: %v", k, v, ok)
			t.FailNow()
		}
	}
	for i := 100_000; i < 150_000; i++ {
		k := strconv.Itoa(i)
		if v, ok := m.Get(k); ok {
			t.Logf("key: %s, value: %v, ok: %v", k, v, ok)
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
			t.Logf("key: %s, value: %v, ok: %v", k, v, ok)
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
			t.Logf("key: %s, value: %v", k, v)
			t.FailNow()
		}
	}
	for i := 100_000; i < 150_000; i++ {
		k := strconv.Itoa(i)
		if v := m.GetOrDefault(k, nil); v != nil {
			t.Logf("key: %s, value: %v", k, v)
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
		k := strconv.Itoa(i)
		if v := m.GetOrDefault(k, nil); v != i {
			t.Logf("key: %s, value: %v", k, v)
			t.FailNow()
		}
	}
	for i := 200_000; i < 300_000; i++ {
		k := strconv.Itoa(i)
		if v := m.GetOrDefault(k, nil); v != nil {
			t.Logf("key: %s, value: %v", k, v)
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
			t.Logf("key: %s, ok: %v", k, ok)
			t.FailNow()
		}
	}
	for i := 100_000; i < 150_000; i++ {
		k := strconv.Itoa(i)
		if ok := m.Contains(k); ok {
			t.Logf("key: %s, ok: %v", k, ok)
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
		k := strconv.Itoa(i)
		if ok := m.Contains(k); !ok {
			t.Logf("key: %s, ok: %v", k, ok)
			t.FailNow()
		}
	}
	for i := 200_000; i < 300_000; i++ {
		k := strconv.Itoa(i)
		if ok := m.Contains(k); ok {
			t.Logf("key: %s, ok: %v", k, ok)
			t.FailNow()
		}
	}
}

func TestConcurrentHashMap_Put_Remove(t *testing.T) {
	m := New()
	for i := 0; i < 100_000; i++ {
		m.Put(strconv.Itoa(i), i)
	}
	for i := 0; i < 100_000; i++ {
		k := strconv.Itoa(i)
		if v, ok := m.Remove(k); !ok && v != i {
			t.Logf("k: %s, v: %v, ok: %v", k, v, ok)
			t.FailNow()
		}
	}
	for i := 0; i < 100_000; i++ {
		k := strconv.Itoa(i)
		if v, ok := m.Remove(k); ok {
			t.Logf("k: %s, v: %v, ok: %v", k, v, ok)
			t.FailNow()
		}
	}
}

func TestConcurrentHashMap_Put_Remove_Verify(t *testing.T) {
	m := New()
	for i := 0; i < 10_000; i++ {
		m.Put(strconv.Itoa(i), i)
	}
	for i := 0; i < 10_000; i++ {
		k := strconv.Itoa(i)
		if v, ok := m.Remove(k); !ok && v != i {
			t.Logf("k: %s, v: %v, ok: %v", k, v, ok)
			t.FailNow()
		}
		if i+1%2_000 == 0 {
			verifyMap(t, &m)
		}
	}
	for i := 0; i < 10_000; i++ {
		k := strconv.Itoa(i)
		if v, ok := m.Remove(k); ok {
			t.Logf("k: %s, v: %v, ok: %v", k, v, ok)
			t.FailNow()
		}
	}
}

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
		k := strconv.Itoa(i)
		if v, ok := m.Remove(k); !ok || v != i {
			t.Logf("k: %s, v: %v, ok: %v", k, v, ok)
			t.FailNow()
		}
	}
	for i := 0; i < 200_000; i++ {
		k := strconv.Itoa(i)
		if v, ok := m.Remove(k); ok {
			t.Logf("k: %s, v: %v, ok: %v", k, v, ok)
			t.FailNow()
		}
	}
}

func TestConcurrentHashMap_ConcurrentlyPut_Remove_Verify(t *testing.T) {
	m := New()
	var wg sync.WaitGroup
	putRange := func(m *ConcurrentHashMap, wg *sync.WaitGroup, from, to int) {
		defer wg.Done()
		for i := from; i < to; i++ {
			m.Put(strconv.Itoa(i), i)
		}
	}
	wg.Add(3)
	go putRange(&m, &wg, 0, 10_000)
	go putRange(&m, &wg, 5_000, 15_000)
	go putRange(&m, &wg, 10_000, 20_000)
	wg.Wait()
	for i := 0; i < 20_000; i++ {
		k := strconv.Itoa(i)
		if v, ok := m.Remove(k); !ok || v != i {
			t.Logf("k: %s, v: %v, ok: %v", k, v, ok)
			t.FailNow()
		}
		if i+1%5_000 == 0 {
			verifyMap(t, &m)
		}
	}
	for i := 0; i < 20_000; i++ {
		k := strconv.Itoa(i)
		if v, ok := m.Remove(k); ok {
			t.Logf("k: %s, v: %v, ok: %v", k, v, ok)
			t.FailNow()
		}
	}
}

func TestConcurrentHashMap_Put_Remove_Contains(t *testing.T) {
	m := New()
	for i := 0; i < 100_000; i++ {
		k := strconv.Itoa(i)
		m.Put(k, i)
		if val, ok := m.Remove(k); !ok || val != i {
			t.FailNow()
		}
		if ok := m.Contains(k); ok {
			t.Logf("k: %s, ok: %v", k, ok)
			t.FailNow()
		}
	}
	for i := 100_000; i < 150_000; i++ {
		k := strconv.Itoa(i)
		if _, ok := m.Remove(k); ok {
			t.Logf("k: %s, ok: %v", k, ok)
			t.FailNow()
		}
	}
}

func TestConcurrentHashMap_Put_Size(t *testing.T) {
	m := New()
	for i := 0; i < 100_000; i++ {
		m.Put(strconv.Itoa(i), i)
	}
	if s := m.Size(); s != 100_000 {
		t.Logf("size: %d", s)
		t.FailNow()
	}
	for i := 50_000; i < 150_000; i++ {
		m.Put(strconv.Itoa(i), i)
	}
	if s := m.Size(); s != 150_000 {
		t.Logf("size: %d", s)
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

func TestConcurrentHashMap_Put_Remove_Size(t *testing.T) {
	m := New()
	for i := 0; i < 100_000; i++ {
		m.Put(strconv.Itoa(i), i)
	}
	if s := m.Size(); s != 100_000 {
		t.Logf("size: %d", s)
		t.FailNow()
	}
	for i := 50_000; i < 150_000; i++ {
		m.Put(strconv.Itoa(i), i)
	}
	if s := m.Size(); s != 150_000 {
		t.Logf("size: %d", s)
		t.FailNow()
	}
	for i := 100_000; i < 200_000; i++ {
		m.Remove(strconv.Itoa(i))
	}
	if s := m.Size(); s != 100_000 {
		t.Logf("size: %d", s)
		t.FailNow()
	}
	for i := 0; i < 100_000; i++ {
		m.Remove(strconv.Itoa(i))
	}
	if s := m.Size(); s != 0 {
		t.Logf("size: %d", s)
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
	if !treeRootNodeVerified(root) {
		t.FailNow()
	}
}

func verifyMap(t *testing.T, m *ConcurrentHashMap) {
	if !mapVerified(m) {
		t.Log("Unverified map")
		t.FailNow()
	}
}

func mapVerified(m *ConcurrentHashMap) bool {
	for i := 0; i < int(m.capacity); i++ {
		if b := m.table[i]; b.tree {
			if !treeRootNodeVerified(b.node) {
				return false
			}
		}
	}
	return true
}

func treeRootNodeVerified(r *node) bool {
	if r == nil {
		return true
	}
	return treeLeafNodeVerified(r) && treeRootNodeVerified(r.left) && treeRootNodeVerified(r.right)
}

func treeLeafNodeVerified(l *node) bool {
	if l == nil {
		return true
	}
	if l.right != nil && l.left != nil {
		return l.hash < l.right.hash && l.hash > l.left.hash && l.right.hash > l.left.hash
	} else if l.right != nil {
		return l.hash < l.right.hash
	} else if l.left != nil {
		return l.hash > l.left.hash
	} else {
		return true
	}
}
