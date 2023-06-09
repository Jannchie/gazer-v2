package gazer

import (
	"testing"

	"github.com/go-redis/redis/v8"
)

type Params struct {
	Name string `json:"name"`
}

type Result struct {
	Data string `json:"data"`
}

func TestFetcher(t *testing.T) {
	db := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	fetcher := NewFetcher[Params, Result](&FetcherOptions[Params, Result]{
		Client: db,
		Key: "test",
		Handler: func(data Params) (*Result, error) {
			t.Log("Fetch:", data.Name)
			return &Result{Data: data.Name}, nil
		}})
	fetcher.Fetch()
}
