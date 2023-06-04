package gazer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"golang.org/x/sync/semaphore"
)

// Fetcher is the client of gazer
type Fetcher[T any, R any] struct {
	client      *redis.Client
	key         string
	name        string
	handler     func(T) (*R, error)
	concurrency uint32
}

type FetcherOptions[T any, R any] struct {
	Client      *redis.Client
	Name        string
	Key         string
	Handler     func(T) (*R, error)
	Concurrency uint32
}

// New create a new gazer client
func NewFetcher[T any, R any](options *FetcherOptions[T, R]) *Fetcher[T, R] {
	if options.Client == nil {
		panic("Redis Client is nil")
	}
	if options.Key == "" {
		panic("Key is empty")
	}
	if options.Name == "" {
		options.Name = "gazer"
	}
	if options.Concurrency == 0 {
		options.Concurrency = 8
	}

	return &Fetcher[T, R]{
		client:      options.Client,
		key:         options.Key,
		name:        options.Name,
		handler:     options.Handler,
		concurrency: options.Concurrency,
	}
}

// getFullRawKey get the full key of the task
func (f Fetcher[T, R]) getFullRawKey(key string) string {
	return f.name + ":raws:" + key
}

func (f Fetcher[T, R]) getFullTaskKey(key string) string {
	return f.name + ":tasks:" + key
}

func (f *Fetcher[T, R]) Fetch() (*R, error) {
	result := f.client.LPop(context.Background(), f.getFullTaskKey(f.key))
	if result.Err() != nil {
		if result.Err().Error() == "redis: nil" {
			<-time.After(time.Second)
			return nil, nil
		}
		return nil, result.Err()
	}

	data := result.Val()
	t := new(T)
	err := json.Unmarshal([]byte(data), t)
	if err != nil {
		return nil, err
	}
	res, err := f.handler(*t)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// RPushTask push task to the tail of the queue
func (f *Fetcher[T, R]) Post(raw *R) {
	rawObj := Raw[*R]{
		Key:       f.key,
		Raw:       raw,
		CreatedAt: time.Now().Unix(),
	}
	data, err := json.Marshal(rawObj)
	if err != nil {
		fmt.Println(err)
		return
	}
	result := f.client.RPush(context.Background(), f.getFullRawKey(f.key), data)
	if result.Err() != nil {
		fmt.Println(result.Err())
	}
}

// Run run the fetcher
func (f *Fetcher[T, R]) Run() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	sem := semaphore.NewWeighted(int64(f.concurrency))
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				if err := sem.Acquire(context.Background(), 1); err != nil {
					fmt.Println(err)
					continue
				}
				go func() {
					defer sem.Release(1)
					res, err := f.Fetch()
					if err != nil {
						fmt.Println(err)
						return
					}
					if res != nil {
						f.Post(res)
					}
				}()
			}
		}
	}()
	<-stop
	fmt.Println("Gazer Fetcher has been stopped.")
}
