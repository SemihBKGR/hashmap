# chmap

[![CI Workflow Badge](https://github.com/SemihBKGR/chmap/actions/workflows/ci.yml/badge.svg)](https://github.com/SemihBKGR/chmap/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/SemihBKGR/chmap)](https://goreportcard.com/report/github.com/SemihBKGR/chmap)
[![Godoc Badge](https://godoc.org/github.com/SemihBKGR/chmap?status.png)](https://pkg.go.dev/github.com/SemihBKGR/chmap)

ConcurrentHashMap implementation in go.

```go
m := chmap.New()

m.Put("nine", 9)

if val, ok := m.Get("nine"); ok {
    _ = val.(int)
    // some code
}

_ = m.GetOrDefault("twenty-three", 23)

if ok := m.Contains("nine"); ok {
    // some code
}

if val, ok := m.Get("nine"); ok {
    _ = val.(int)
    // some code
}
```
