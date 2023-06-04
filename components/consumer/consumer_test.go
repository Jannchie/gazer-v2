package gazer

import (
	"testing"

	"github.com/go-redis/redis/v8"
)

type Result struct {
	Data string `json:"data"`
}

func TestConsumer(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	consumer := NewConsumer[Result](&GazerConsumerOption[Result]{
		Client: client,
		Key:    "test",
		Handler: func(data *Result) error {
			t.Log("Consume:", data.Data)
			return nil
		},
	})
	consumer.Consume()
}
