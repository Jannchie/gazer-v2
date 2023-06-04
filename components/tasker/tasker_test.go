package gazer

import (
	"context"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/jannchie/gazer-v2/common/models"
)

type Data struct {
	Name string `json:"name"`
}

func TestClient(t *testing.T) {
	db := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	client := NewTasker[Data](&GazerTaskerOptions{
		Client: db,
	})
	err := client.RPushTask(context.Background(), &models.Task[Data]{
		Key:    "test",
		Params: Data{Name: "This is a test"},
	})
	if err != nil {
		t.Error(err)
	}
}
