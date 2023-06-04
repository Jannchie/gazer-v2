package gazer

type Raw[T any] struct {
	Key       string `json:"key"`
	CreatedAt int64  `json:"created_at"`
	Raw       T      `json:"raw"`
}
