package chmap

import (
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
