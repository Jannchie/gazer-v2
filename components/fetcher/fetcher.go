package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/jannchie/gazer-v2/common/pool"
)

// GazerFetcher is the client of gazer
type GazerFetcher[T any, R any] struct {
	client      *redis.Client
	key         string
	name        string
	handler     func(T) (R, error)
	concurrency uint32
	pool        *pool.Pool[T, R]
}

type GazerFetcherOptions[T any, R any] struct {
	Client      *redis.Client
	Name        string
	Key         string
	Handler     func(T) (R, error)
	Concurrency uint32
}

// New create a new gazer client
func New[T any, R any](options *GazerFetcherOptions[T, R]) *GazerFetcher[T, R] {
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

	return &GazerFetcher[T, R]{
		client:      options.Client,
		key:         options.Key,
		name:        options.Name,
		handler:     options.Handler,
		concurrency: options.Concurrency,
		pool:        pool.New[T, R](options.Handler, options.Concurrency),
	}
}

// getFullRawKey get the full key of the task
func (f GazerFetcher[T, R]) getFullRawKey(key string) string {
	return f.name + ":raws:" + key
}

func (f GazerFetcher[T, R]) getFullTaskKey(key string) string {
	return f.name + ":tasks:" + key
}

func (f *GazerFetcher[T, R]) Fetch() (*R, error) {
	result := f.client.LPop(context.Background(), f.key)
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
	f.pool.Dispatch(*t)
	res := <-f.pool.Results()
	if res.Err != nil {
		return nil, res.Err
	}
	return &res.Result, nil
}

// RPushTask push task to the tail of the queue
func (f *GazerFetcher[T, R]) Post(raw *R) {
	data, err := json.Marshal(raw)
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
func (f *GazerFetcher[T, R]) Run() {
	f.pool.Start()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				res, err := f.Fetch()
				if err != nil {
					fmt.Println(err)
					continue
				}
				if res != nil {
					f.Post(res)
				}
			}
		}
	}()
	<-stop
	fmt.Println("Gazer Fetcher has been stopped.")
}
