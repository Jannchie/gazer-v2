package gazer

import (
	"testing"

	"github.com/go-redis/redis/v8"
)

type TestResult struct {
	Data string `json:"data"`
}

func TestConsumer(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	consumer := NewConsumer[TestResult](&ConsumerOption[TestResult]{
		Client: client,
		Key:    "test",
		Handler: func(data *Raw[TestResult]) error {
			t.Log("Consume:", data.Raw.Data)
			return nil
		},
	})
	consumer.Consume()
}
