package gazer

type Task[T any] struct {
	Key    string `json:"key"`
	Params T      `json:"params"`
}
