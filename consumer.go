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
)

type Consumer[R any] struct {
	client  *redis.Client
	name    string
	key     string
	handler func(*Raw[R]) error
}

type ConsumerOption[R any] struct {
	Client  *redis.Client
	Name    string
	Key     string
	Handler func(*Raw[R]) error
}

func NewConsumer[R any](options *ConsumerOption[R]) *Consumer[R] {
	if options.Client == nil {
		panic("Redis Client is nil")
	}
	if options.Key == "" {
		panic("Key is empty")
	}
	if options.Name == "" {
		options.Name = "gazer"
	}
	return &Consumer[R]{
		client:  options.Client,
		key:     options.Key,
		name:    options.Name,
		handler: options.Handler,
	}
}

func (c Consumer[R]) getFullKey(key string) string {
	return c.name + ":raws:" + key
}

func (c *Consumer[R]) Consume() error {
	result := c.client.LPop(context.Background(), c.getFullKey(c.key))
	if result.Err() != nil {
		if result.Err().Error() == "redis: nil" {
			<-time.After(time.Second)
			return nil
		}
		return result.Err()
	}
	data := result.Val()
	if data == "" {
		return nil
	}
	t := new(Raw[R])
	err := json.Unmarshal([]byte(data), t)
	if err != nil {
		return err
	}
	err = c.handler(t)
	if err != nil {
		return err
	}
	return nil
}

// Run run the fetcher
func (c *Consumer[R]) Run() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				err := c.Consume()
				if err != nil {
					fmt.Println(err)
					continue
				}
			}
		}
	}()
	<-stop
	fmt.Println("Gazer Consumer has been stopped.")
}
