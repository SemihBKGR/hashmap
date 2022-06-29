package chmap

import "math/rand"

func Example_string_map() {
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
}

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

func Example_custom_map() {

	m := New[User, UserContext]()

	user := User{
		ID:   1,
		Name: "alice",
	}

	userContext := make(map[string]any)
	userContext["sessionID"] = rand.Int()

	m.Put(user, userContext)

	if val, ok := m.Get(User{ID: 1}); ok {
		_ = val["sessionID"]
		// ...
	}

}
