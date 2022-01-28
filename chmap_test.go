package chmap

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
)

func TestNew(t *testing.T) {
	_ = New()
}

func TestConcurrentHashMap_Put(t *testing.T) {
	m := New()
	for i := 0; i < 1_000; i++ {
		m.Put(strconv.Itoa(i), i)
	}
}

func TestConcurrentHashMap_Get(t *testing.T) {
	m := New()
	for i := 0; i < 1_000; i++ {
		m.Put(strconv.Itoa(i), i)
		val, ok := m.Get(strconv.Itoa(i))
		if !ok || val != i {
			t.FailNow()
		}
	}
}

func TestConcurrentHashMap_Contains(t *testing.T) {
	m := New()
	for i := 0; i < 1_000; i++ {
		m.Put(strconv.Itoa(i), i)
		ok := m.Contains(strconv.Itoa(i))
		if !ok {
			t.FailNow()
		}
	}
}

func TestConcurrentHashMap_Remove(t *testing.T) {
	m := New()
	for i := 0; i < 1_000; i++ {
		m.Put(strconv.Itoa(i), i)
		val, ok := m.Remove(strconv.Itoa(i))
		if !ok || val != i {
			t.FailNow()
		}
		ok = m.Contains(strconv.Itoa(i))
		if ok {
			t.FailNow()
		}
	}
}

func TestMakeTree(t *testing.T) {
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
	printHashOfNodeLink(head)
	printSize(head)
	root := makeTree(head)
	if !validTree(root) {
		t.FailNow()
	}

}

func printHashOfNodeLink(head *node) {
	fmt.Print("[")
	n := head
	for {
		fmt.Print(n.hash)
		n = n.right
		if n == nil {
			break
		}
		fmt.Print(",")
	}
	fmt.Println("]")
}

func printSize(head *node) {
	n := head
	s := 0
	for n != nil {
		s++
		n = n.right
	}
	fmt.Printf("Size: %d\n", s)
}

func validTree(root *node) bool {
	if root == nil {
		return true
	}
	return validTreeNode(root) && validTree(root.left) && validTree(root.right)
}

func validTreeNode(n *node) bool {
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
