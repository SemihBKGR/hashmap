# chmap

[![CI Workflow Badge](https://github.com/SemihBKGR/chmap/actions/workflows/ci.yml/badge.svg)](https://github.com/SemihBKGR/chmap/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/SemihBKGR/chmap)](https://goreportcard.com/report/github.com/SemihBKGR/chmap)
[![Godoc Badge](https://godoc.org/github.com/SemihBKGR/chmap?status.png)](https://pkg.go.dev/github.com/SemihBKGR/chmap)

ConcurrentHashMap implementation in go.

It supports generic type, so go version 1.18 is required.

```bash
go get github.com/semihbkgr/chmap@latest
```

string type key map example

```go
m := NewString[int]()

m.Put("nine", 9)

if val, ok := m.Get("nine"); ok {
    _ = val + 11
    // ...
}

_ = m.GetOrDefault("twenty-three", 23)

if ok := m.Contains("nine"); ok {
    // ...
}
```

custom type key map example

```go
type User struct {
    ID   uint
    Name string
}

func (u User) Hash() uint32 {
    return uint32(u.ID)
}

func (u User) Equals(a any) bool {
    user, ok := a.(User)
    if !ok {
        return false
    }
    return u.ID == user.ID
}

type UserContext map[string]any

func main() {

    m := New[User, UserContext]()

    user := User{
        ID:   1,
        Name: "alice",
    }

    userContext:=make(map[string]any)
    userContext["session"]=sessionId()

    m.Put(user, userContext)

    if val, ok := m.Get(User{ID: 1}); ok {
        _ = val["session"]
        // ...
    }

}
```
