package chmap

import (
	"math/rand"
	"strconv"
	"testing"
)

func TestNew(t *testing.T) {
	_ = New()
}

func TestNewWithCap(t *testing.T) {
	_, err := NewWithCap(32)
	if err != nil {
		t.FailNow()
	}
}

func TestNewWithCap2(t *testing.T) {
	_, err := NewWithCap(0)
	if err == nil {
		t.FailNow()
	}
}

func TestNewWithCap3(t *testing.T) {
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

func TestConcurrentHashMap_Get(t *testing.T) {
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

func TestConcurrentHashMap_GetOrDefault(t *testing.T) {
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

func TestConcurrentHashMap_Contains(t *testing.T) {
	m := New()
	for i := 0; i < 100_000; i++ {
		k := strconv.Itoa(i)
		m.Put(k, i)
		if ok := m.Contains(k); !ok {
			t.FailNow()
		}
	}
	for i := 100_000; i < 150_000; i++ {
		k := strconv.Itoa(i)
		if ok := m.Contains(k); ok {
			t.FailNow()
		}
	}
}

func TestConcurrentHashMap_Remove(t *testing.T) {
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

func TestConcurrentHashMap_Size(t *testing.T) {
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

func isTreeRootAsExpected(root *node) bool {
	if root == nil {
		return true
	}
	return isTreeLeafAsExpected(root) && isTreeRootAsExpected(root.left) && isTreeRootAsExpected(root.right)
}

func isTreeLeafAsExpected(n *node) bool {
	if n == nil {
		return true
	} else {
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
}
